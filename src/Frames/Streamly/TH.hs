{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RoleAnnotations #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
-- | Code generation of types relevant to Frames use-cases. Generation
-- may be driven by an automated inference process or manual use of
-- the individual helpers.
module Frames.Streamly.TH
  (
    -- * Declare Individual Column Types
    declareColumn
  , declareColumnType
  , declarePrefixedColumn
  , declarePrefixedColumnType
    -- * Control Type-declarations for a table
  , RowGen(..)
  , defaultSep
  , defaultIsMissing
  , OrMissingWhen(..)
  , setOrMissingWhen
  , rowGen
  , rowGenCat
  , modifyColumnSelector
  , modifyRowTypeNameAndColumnSelector
  , allColumnsAsNamed
  , noHeaderColumnsNumbered
  , prefixColumns
  , prefixAsNamed
  , columnSubset
  , renamedHeaderSubset
  , renameSomeUsingNames
  , renameSomeUsingPositions
  , namedColumnNumberSubset
  , namesGiven
    -- * Declare all necessary types for a table
  , tableTypes
  , tableTypes'
  , tableTypesText'
  , ColumnId(..)
    -- * Header text vs. Type name text
  , ColTypeName(..)
  , HeaderText(..)
    -- * Re-exports
  , module Frames.Streamly.OrMissing
  )
where

import qualified Frames.Streamly.CSV as SCSV
import qualified Frames.Streamly.Internal.CSV as ICSV
import Frames.Streamly.Internal.CSV (ColumnId(..), HeaderText(..) ,ColTypeName(..), OrMissingWhen(..))
import Frames.Streamly.CSV (ParserOptions(..))
import Frames.Streamly.OrMissing
import qualified Frames.Streamly.ColumnUniverse as FSCU
import qualified Frames.Streamly.ColumnTypeable as FSCT

import Prelude hiding (Type, lift)
import Data.Char (toLower)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Vinyl
import Data.Vinyl.TypeLevel (RIndex)
import Frames.Col ((:->))
import Frames.ColumnTypeable
import Frames.ColumnUniverse
import Frames.CSV hiding (ParserOptions(..))
import Frames.Rec(Record)
import Frames.Utils
import qualified GHC.Types as GHC
import Language.Haskell.TH
import Language.Haskell.TH.Syntax

import qualified Streamly.Prelude as Streamly
#if MIN_VERSION_streamly(0,8,0)
#else
import qualified Streamly
#endif

-- | Generate a column type.
recDec :: [Type] -> Type
recDec = AppT (ConT ''Record) . go
  where go [] = PromotedNilT
        go (t:cs) = AppT (AppT PromotedConsT t) (go cs)

-- | Declare a type synonym for a column.
mkColSynDec :: TypeQ -> Name -> DecQ
mkColSynDec colTypeQ colTName = tySynD colTName [] colTypeQ

-- | Declare lenses for working with a column.
mkColLensDec :: Name -> Type -> T.Text -> DecsQ
mkColLensDec colTName colTy colPName = sequenceA [tySig, val, tySig', val']
  where nm = mkName $ T.unpack colPName
        nm' = mkName $ T.unpack colPName <> "'"
        -- tySig = sigD nm [t|Proxy $(conT colTName)|]
        tySig = sigD nm [t|forall f rs.
                           (Functor f,
                            RElem $(conT colTName) rs (RIndex $(conT colTName) rs))
                         => ($(pure colTy) -> f $(pure colTy))
                         -> Record rs
                         -> f (Record rs)
                         |]
        tySig' = sigD nm' [t|forall f g rs.
                            (Functor f,
                             RElem $(conT colTName) rs (RIndex $(conT colTName) rs))
                          => (g $(conT colTName) -> f (g $(conT colTName)))
                          -> Rec g rs
                          -> f (Rec g rs)
                          |]
        val = valD (varP nm)
                   (normalB [e|rlens @($(conT colTName)) . rfield |])
                   []
        val' = valD (varP nm')
                    (normalB [e|rlens' @($(conT colTName))|])
                    []

lowerHead :: T.Text -> Maybe T.Text
lowerHead = fmap aux . T.uncons
  where aux (c,t) = T.cons (toLower c) t

-- | For each column, we declare a type synonym for its type, and a
-- Proxy value of that type.
colDec :: T.Text -> String -> T.Text -> Bool
       -> Either (String -> Q [Dec]) Type
       -> Q (Type, [Dec])
colDec prefix rowName colName addOrMissing colTypeGen = do
  (colTy', extraDecs) <- either colDecsHelper (pure . (,[])) colTypeGen
  let colTy = addOrMissingToTypeIf addOrMissing colTy'
      colTypeQ = [t|$(litT . strTyLit $ T.unpack colName) :-> $(return colTy)|]
  syn <- mkColSynDec colTypeQ colTName'
  lenses <- mkColLensDec colTName' colTy colPName
  return (ConT colTName', syn : extraDecs ++ lenses)
  where addOrMissingToTypeIf :: Bool -> Type -> Type
        addOrMissingToTypeIf b t = if b then AppT (ConT ''OrMissing) t else t
        colTName = sanitizeTypeName (prefix <> capitalize1 colName)
        colPName = fromMaybe "colDec impossible" (lowerHead colTName)
        colTName' = mkName $ T.unpack colTName
        colDecsHelper f =
          let qualName = rowName ++ T.unpack (capitalize1 colName)
          in (ConT (mkName qualName),) <$> f qualName

-- | Splice for manually declaring a column of a given type. For
-- example, @declareColumn "x2" ''Double@ will declare a type synonym
-- @type X2 = "x2" :-> Double@ and a lens @x2@.
declareColumn :: T.Text -> Name -> DecsQ
declareColumn = flip declarePrefixedColumn T.empty

-- | Splice for manually declaring a column of a given type. Works
-- like 'declareColumn', but uses a quote for the payload type
-- rather than a specific 'Name'. This lets you declare a column with
-- a type like, @Maybe Int@, where @Maybe Int@ is a 'Type' but not a
-- 'Name'. For example, with @-XOverloadedStrings@ and
-- @-XQuasiQuotes@,
--
-- > declareColumnType "x2" [t|Maybe Int|]
declareColumnType :: T.Text -> Q Type -> DecsQ
declareColumnType n = declarePrefixedColumnType n T.empty


-- | Splice for manually declaring a column of a given type in which
-- the generated type synonym's name has a prefix applied to the
-- column name. For example, @declarePrefixedColumn "x2" "my"
-- ''Double@ will declare a type synonym @type MyX2 = "x2" :-> Double@
-- and a lens @myX2@.
declarePrefixedColumn :: T.Text -> T.Text -> Name -> DecsQ
declarePrefixedColumn colName prefix colTypeName =
  (:) <$> mkColSynDec colTypeQ colTName'
      <*> mkColLensDec colTName' colTy colPName
  where prefix' = capitalize1 prefix
        colTName = sanitizeTypeName (prefix' <> capitalize1 colName)
        colPName = fromMaybe "colDec impossible" (lowerHead colTName)
        colTName' = mkName $ T.unpack colTName
        colTy = ConT colTypeName
        colTypeQ = [t|$(litT . strTyLit $ T.unpack colName) :-> $(return colTy)|]

-- | Splice for manually declaring a column of a given type. Works
-- like 'declarePrefixedColumn', but uses a quote for the payload type
-- rather than a specific 'Name'. This lets you declare a column with
-- a type like, @Maybe Int@, where @Maybe Int@ is a 'Type' but not a
-- 'Name'. For example, with @-XOverloadedStrings@ and
-- @-XQuasiQuotes@,
--
-- > declarePrefixedColumnType "x2" "my" [t|Maybe Int|]
declarePrefixedColumnType :: T.Text -> T.Text -> Q Type -> DecsQ
declarePrefixedColumnType colName prefix payloadType =
  (:) <$> mkColSynDec colTypeQ colTName'
      <*> (payloadType >>= \colTy -> mkColLensDec colTName' colTy colPName)
  where prefix' = capitalize1 prefix
        colTName = sanitizeTypeName (prefix' <> capitalize1 colName)
        colPName = fromMaybe "colDec impossible" (lowerHead colTName)
        colTName' = mkName $ T.unpack colTName
        colTypeQ = [t|$(litT . strTyLit $ T.unpack colName) :-> $payloadType|]

-- * Default CSV Parsing

-- | Control how row and named column types are generated.
-- The first type argument is @Text@ or @Int@ depending how columns are indexed.
-- The second type argument is a type-level list of the possible column types.
data RowGen (b :: ColumnId) (a :: [GHC.Type]) =
  RowGen { genColumnSelector    :: ICSV.RowGenColumnSelector b
           -- ^ Use these column names. If empty, expect a
           -- header row in the data file to provide
           -- column names.
         , tablePrefix    :: String
           -- ^ A common prefix to use for every generated
           -- declaration.
         , separator      :: Separator
           -- ^ The string that separates the columns on a
           -- row.
         , rowTypeName    :: String
           -- ^ The row type that enumerates all
           -- columns.
         , columnUniverse :: Proxy a
           -- ^ A record field that mentions the phantom type list of
           -- possible column types. Having this field prevents record
           -- update syntax from losing track of the type argument.
         , inferencePrefix :: Int
           -- ^ Number of rows to inspect to infer a type for each
           -- column. Defaults to 1000.
         , isMissing :: Text -> Bool
           -- ^ Control what text is considered missing.
           -- Defaults to @isMissing t = null t || t == "NA"@
         , lineReader :: Separator -> Streamly.SerialT IO [Text]
           -- ^ A producer of rows of ’T.Text’ values that were
           -- separated by a 'Separator' value.
         }

defaultIsMissing :: Text -> Bool
defaultIsMissing t = T.null t || t == "NA"

-- | A default 'RowGen'. This instructs the type inference engine to
-- get column names from the data file, use the default column
-- separator (a comma), infer column types from the default 'Columns'
-- set of types, and produce a row type with name @Row@.
rowGen :: FilePath -> RowGen 'ColumnByName FSCU.CommonColumns
rowGen = RowGen allColumnsAsNamed "" defaultSep "Row" Proxy 1000 defaultIsMissing . SCSV.streamTokenized'
{-# INLINEABLE rowGen #-}

-- | Like 'rowGen', but will also generate custom data types for
-- 'Categorical' variables with up to 8 distinct variants.
rowGenCat :: FilePath -> RowGen 'ColumnByName FSCU.CommonColumnsCat
rowGenCat = RowGen allColumnsAsNamed "" defaultSep "Row" Proxy 1000 defaultIsMissing . SCSV.streamTokenized'
{-# INLINEABLE rowGenCat #-}

-- | Update or replace the columnHandler in a RowGen
modifyColumnSelector :: forall a b x.(ICSV.RowGenColumnSelector a -> ICSV.RowGenColumnSelector b) -> RowGen a x -> RowGen b x
modifyColumnSelector f rg =
  let newColHandler :: ICSV.RowGenColumnSelector b = f (genColumnSelector rg)
  in rg { genColumnSelector = newColHandler }
{-# INLINEABLE modifyColumnSelector #-}

-- | Update or replace the columnHandler in a RowGen
modifyRowTypeNameAndColumnSelector :: Text -> (ICSV.RowGenColumnSelector a -> ICSV.RowGenColumnSelector b) -> RowGen a x -> RowGen b x
modifyRowTypeNameAndColumnSelector newRowName f rg =
  let newColHandler = f (genColumnSelector rg)
  in rg { rowTypeName = T.unpack newRowName, genColumnSelector = newColHandler }
{-# INLINEABLE modifyRowTypeNameAndColumnSelector #-}


-- | Default Column Handler. Declare one type per column.
-- Use the header to generate names.
allColumnsAsNamed :: ICSV.RowGenColumnSelector 'ColumnByName
allColumnsAsNamed = ICSV.GenUsingHeader (\x -> ICSV.Include (ICSV.ColTypeName $ ICSV.headerText x, ICSV.NeverMissing)) (const [])
{-# INLINEABLE allColumnsAsNamed #-}

-- | Helper for declaring column types from a file with no header.
noHeaderColumnsNumbered' :: ICSV.RowGenColumnSelector 'ColumnByPosition
noHeaderColumnsNumbered' = ICSV.GenWithoutHeader (\n -> ICSV.Include (ICSV.ColTypeName $ show n, ICSV.NeverMissing)) (const [])
{-# INLINEABLE noHeaderColumnsNumbered' #-}

-- | Use a given prefix and append the column number to generate column types for a file with no header.
noHeaderColumnsNumbered :: Text -> ICSV.RowGenColumnSelector 'ColumnByPosition
noHeaderColumnsNumbered prefix = prefixColumns prefix noHeaderColumnsNumbered'
{-# INLINEABLE noHeaderColumnsNumbered #-}

-- | Add a prefix to all the generated column type names.
prefixColumns :: Text -> ICSV.RowGenColumnSelector a -> ICSV.RowGenColumnSelector a
prefixColumns p ch = ICSV.modifyColumnSelector ch g id where
  g f = \x -> case f x of
    ICSV.Exclude -> ICSV.Exclude
    ICSV.Include (t, mw) -> ICSV.Include (ICSV.ColTypeName (p <> ICSV.colTypeName t), mw)
{-# INLINEABLE prefixColumns #-}

-- | Generate all column type names from the header but add a prefix.
prefixAsNamed :: Text -> ICSV.RowGenColumnSelector 'ColumnByName
prefixAsNamed p = prefixColumns p allColumnsAsNamed
{-# INLINEABLE prefixAsNamed #-}

inSetButNotList :: Ord a => Set a -> [a] -> [a]
inSetButNotList s = Set.toList . Set.difference s . Set.fromList
{-# INLINE inSetButNotList #-}

inMapKeysButNotList :: Ord a => Map a v -> [a] -> [a]
inMapKeysButNotList m = inSetButNotList (Map.keysSet m)
{-# INLINE inMapKeysButNotList #-}

-- | Generate types for only a subset of the columns.
-- Generated 'ParserOptions' will select the correct columns when loading.
columnSubset :: Ord (ICSV.ColumnIdType a) => Set (ICSV.ColumnIdType a) -> ICSV.RowGenColumnSelector a -> ICSV.RowGenColumnSelector a
columnSubset s rgch = ICSV.modifyColumnSelector rgch g h where
  g f = \x -> if x `Set.member` s then f x else ICSV.Exclude
  h mrF cids = mrF cids ++ inSetButNotList s cids
{-# INLINEABLE columnSubset #-}

-- | Generate Column Type Names for only the headers given in the map.  Use
-- the names given as values in the map rather than those in the header.
-- Generated 'ParserOptions' will select the correct columns when loading.
renamedHeaderSubset :: Map ICSV.HeaderText ICSV.ColTypeName -> ICSV.RowGenColumnSelector 'ColumnByName
renamedHeaderSubset renamedS = ICSV.GenUsingHeader f mrF where
  f x = maybe ICSV.Exclude g $ Map.lookup x renamedS
  mrF = inMapKeysButNotList renamedS
  g x = ICSV.Include (x, ICSV.NeverMissing)
{-# INLINEABLE renamedHeaderSubset #-}


-- | rename some column types while leaving the rest alone.
renameSome ::  Ord (ICSV.ColumnIdType a)
           => Map (ICSV.ColumnIdType a) ICSV.ColTypeName --, ICSV.OrMissingWhen)
           -> ICSV.RowGenColumnSelector a
           -> ICSV.RowGenColumnSelector a
renameSome m rgcs = ICSV.modifyColumnSelector rgcs g h where
  wom = \case
    ICSV.Exclude -> NeverMissing
    ICSV.Include (_, x) -> x
  g f cid = case Map.lookup cid m of
    Nothing -> f cid
    Just x -> ICSV.Include (x, wom $ f cid)
  h mrF cids = mrF cids ++ inMapKeysButNotList m cids
{-# INLINEABLE renameSome #-}

-- | rename some column types while leaving the rest alone.
renameSomeUsingNames :: Map ICSV.HeaderText ICSV.ColTypeName --, ICSV.OrMissingWhen)
                     -> ICSV.RowGenColumnSelector 'ICSV.ColumnByName
                     -> ICSV.RowGenColumnSelector 'ICSV.ColumnByName
renameSomeUsingNames = renameSome
{-# INLINEABLE renameSomeUsingNames #-}

-- | rename some column types while leaving the rest alone.
renameSomeUsingPositions :: Map Int ICSV.ColTypeName
                         -> ICSV.RowGenColumnSelector 'ICSV.ColumnByPosition
                         -> ICSV.RowGenColumnSelector 'ICSV.ColumnByPosition
renameSomeUsingPositions = renameSome
{-# INLINEABLE renameSomeUsingPositions #-}

-- | Generate Column Type Names for only the given numbered Columns using
-- names given as values in the map.  Can ignore a header row or work without one
-- as set by the first parameter.
-- Generated 'ParserOptions' will select the correct columns when loading.
namedColumnNumberSubset :: Bool -> Map Int ICSV.ColTypeName -> ICSV.RowGenColumnSelector 'ColumnByPosition
namedColumnNumberSubset hasHeader namedS =
  if hasHeader
    then ICSV.GenIgnoringHeader f mrF
    else ICSV.GenWithoutHeader f mrF
  where
    f n = maybe ICSV.Exclude g $ Map.lookup n namedS
    g x = ICSV.Include (x, ICSV.NeverMissing)
    mrF = inMapKeysButNotList namedS
{-# INLINEABLE namedColumnNumberSubset #-}

-- | Use the given names to declare Column Type Names for the
-- first N (= length of the given list of names) columns of the given file.
-- Generated 'ParserOptions' will select the correct columns when loading.
namesGiven :: Bool -> [ICSV.ColTypeName] -> ICSV.RowGenColumnSelector 'ColumnByPosition
namesGiven hasHeader names = namedColumnNumberSubset hasHeader m
  where
    m = Map.fromList $ zip [0..] names
{-# INLINEABLE namesGiven #-}

setOrMissingWhen :: (Eq (ICSV.ColumnIdType b)) => ICSV.ColumnIdType b -> ICSV.OrMissingWhen -> RowGen b a -> RowGen b a
setOrMissingWhen cid mw rg = rg { genColumnSelector = newColSelector } where
  colSelector = genColumnSelector rg
  f oldF x = if x /= cid
             then oldF x
             else case oldF x of
                    ICSV.Exclude -> ICSV.Exclude
                    ICSV.Include (ctn, _) -> ICSV.Include (ctn, mw)
  newColSelector = ICSV.modifyColumnSelector colSelector f id
{-# INLINEABLE setOrMissingWhen #-}

-- -- | Generate a type for each row of a table. This will be something
-- -- like @Record ["x" :-> a, "y" :-> b, "z" :-> c]@.
-- tableType :: String -> FilePath -> DecsQ
-- tableType n fp = tableType' (rowGen fp) { rowTypeName = n }

-- | Like 'tableType', but additionally generates a type synonym for
-- each column, and a proxy value of that type. If the CSV file has
-- column names \"foo\", \"bar\", and \"baz\", then this will declare
-- @type Foo = "foo" :-> Int@, for example, @foo = rlens \@Foo@, and
-- @foo' = rlens' \@Foo@.
tableTypes :: String -> FilePath -> DecsQ
tableTypes n fp = tableTypes' (rowGen fp) { rowTypeName = n }

-- * Customized Data Set Parsing

-- | Generate a type for a row of a table. This will be something like
-- @Record ["x" :-> a, "y" :-> b, "z" :-> c]@.  Column type synonyms
-- are /not/ generated (see 'tableTypes'').
-- tableType' :: forall a. (ColumnTypeable a, Monoid a)
--            => RowGen a -> DecsQ
-- tableType' (RowGen {..}) =
--     pure . TySynD (mkName rowTypeName) [] <$>
--     (runIO (P.runSafeT (readColHeaders opts lineSource)) >>= recDec')
--   where recDec' = recDec . map (second colType) :: [(T.Text, a)] -> Q Type
--         colNames' | null columnNames = Nothing
--                   | otherwise = Just (map T.pack columnNames)
--         opts = ParserOptions colNames' separator (RFC4180Quoting '\"')
--         lineSource = lineReader separator >-> P.take prefixSize

-- | Tokenize the first line of a ’Streamly.SerialT’.
colNamesP :: Monad m => Streamly.SerialT m [T.Text] -> m [T.Text]
colNamesP src = fromMaybe [] <$> Streamly.head src

--mkRowFilter :: (Text -> Bool) -> [Bool]
--mkRowFilter hs = fmap (\f -> fmap f hs)


-- | Generate a type for a row of a table all of whose columns remain
-- unparsed 'Text' values.
tableTypesText' :: forall a b c.
                   (c ~ CoRec ColInfo a, ColumnTypeable c, Monoid c)
                => RowGen b a -> DecsQ
tableTypesText' RowGen {..} = do
  firstRow <- runIO $ colNamesP (lineReader separator)
  let (allColStates, pch) = case genColumnSelector of
        ICSV.GenUsingHeader f _ ->
          let allHeaders = ICSV.HeaderText <$> firstRow
              allColStates' = f <$> allHeaders
          in (allColStates', ICSV.colStatesAndHeadersToParseColHandler allColStates' allHeaders)
        ICSV.GenIgnoringHeader f _ ->
          let allHeaders = ICSV.HeaderText <$> firstRow
              allIndexes = [0..(length allHeaders - 1)]
              allColStates' = f <$> allIndexes
          in (allColStates', ICSV.ParseIgnoringHeader allColStates')
        ICSV.GenWithoutHeader f _ ->
          let allIndexes = [0..(length firstRow - 1)]
              allColStates' = f <$> allIndexes
          in (allColStates', ICSV.ParseWithoutHeader allColStates')
      colNames = ICSV.includedColTypeNames allColStates

  let opts = ParserOptions pch separator (RFC4180Quoting '\"')
  let colNamesT = zip (fmap ICSV.colTypeName colNames) (repeat (ConT ''T.Text))
  (colTypes, colDecs) <- second concat . unzip
                         <$> mapM (uncurry mkColDecs) colNamesT
  let recTy = TySynD (mkName rowTypeName) [] (recDec colTypes)
      optsName = case rowTypeName of
                   [] -> error "Row type name shouldn't be empty"
                   h:t -> mkName $ toLower h : t ++ "Parser"
  optsTy <- sigD optsName [t|ParserOptions|]
  optsDec <- valD (varP optsName) (normalB $ lift opts) []
  return (recTy : optsTy : optsDec : colDecs)
  where mkColDecs colNm colTy = do
          let safeName = T.unpack (sanitizeTypeName colNm)
          mColNm <- lookupTypeName (tablePrefix ++ safeName)
          case mColNm of
            Just n -> pure (ConT n, [])
            Nothing -> colDec (T.pack tablePrefix) rowTypeName colNm False (Right colTy)


-- | Generate a type for a row of a table. This will be something like
-- @Record ["x" :-> a, "y" :-> b, "z" :-> c]@. Additionally generates
-- a type synonym for each column, and a proxy value of that type. If
-- the CSV file has column names \"foo\", \"bar\", and \"baz\", then
-- this will declare @type Foo = "foo" :-> Int@, for example, @foo =
-- rlens \@Foo@, and @foo' = rlens' \@Foo@.
tableTypes' :: forall ts b.
               (RMap ts
               , RApply ts
               , RFoldMap ts
               , RecApplicative ts
               , RPureConstrained FSCT.Parseable ts
               , Show (ICSV.ColumnIdType b))
            => RowGen b ts -> DecsQ
tableTypes' (RowGen {..}) = do
  (typedCols, pch) <- runIO $ SCSV.readColHeaders genColumnSelector lineSource :: Q ([SCSV.ColTypeInfo ts], ICSV.ParseColumnSelector)
--  let colDecsFromTypedCols :: (ICSV.ColTypeName, FSCU.ColType ts)
--      colDecsFromTypedCols
  (colTypes, colDecs) <- (second concat . unzip)
                         <$> mapM mkColDecs typedCols
  let recTy = TySynD (mkName rowTypeName) [] (recDec colTypes)
      opts = ParserOptions pch separator (RFC4180Quoting '\"')
      optsName = case rowTypeName of
                   [] -> error "Row type name shouldn't be empty"
                   h:t -> mkName $ toLower h : t ++ "Parser"
  optsTy <- sigD optsName [t|ParserOptions|]
  optsDec <- valD (varP optsName) (normalB $ lift opts) []
  return (recTy : optsTy : optsDec : colDecs)
     -- (:) <$> (tySynD (mkName n) [] (recDec' headers))
     --     <*> (concat <$> mapM (uncurry $ colDec (T.pack prefix)) headers)
  where lineSource :: Streamly.SerialT IO [Text]
        lineSource = Streamly.take inferencePrefix $ lineReader separator --P.>-> P.take inferencePrefix
        inferMaybe :: ICSV.OrMissingWhen -> FSCU.SomeMissing -> Bool
        inferMaybe mw sm = case mw of
          ICSV.NeverMissing -> False
          ICSV.AlwaysPossible -> True
          ICSV.IfSomeMissing -> sm == FSCU.SomeMissing
        mkColDecs :: SCSV.ColTypeInfo ts -> Q (Type, [Dec])
        mkColDecs (SCSV.ColTypeInfo colNm colMW colTy) = do
          let safeName = tablePrefix ++ (T.unpack . sanitizeTypeName . ICSV.colTypeName $ colNm) -- ??
              colTyTH = FSCU.colTypeTH colTy
              isMaybe = inferMaybe colMW (FSCU.colTypeSomeMissing colTy)
          mColNm <- lookupTypeName safeName
          case mColNm of
            Just n -> pure (ConT n, []) -- Column's type was already defined
            Nothing -> colDec (T.pack tablePrefix) rowTypeName (ICSV.colTypeName colNm) isMaybe colTyTH
