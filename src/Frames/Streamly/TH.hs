{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
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
    module Frames.Streamly.TH
  )
where

import qualified Frames.Streamly.CSV as SCSV
import Frames.Streamly.Internal.CSV as ICSV
--import Frames.Streamly.Internal.CSV (RowGenColumnHandler(..))
import Frames.Streamly.CSV (ParserOptions(..))

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

--import qualified Pipes as P
--import qualified Pipes.Prelude as P
--import qualified Pipes.Safe as P

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
colDec :: T.Text -> String -> T.Text
       -> (Either (String -> Q [Dec]) Type)
       -> Q (Type, [Dec])
colDec prefix rowName colName colTypeGen = do
  (colTy, extraDecs) <- either colDecsHelper (pure . (,[])) colTypeGen
  let colTypeQ = [t|$(litT . strTyLit $ T.unpack colName) :-> $(return colTy)|]
  syn <- mkColSynDec colTypeQ colTName'
  lenses <- mkColLensDec colTName' colTy colPName
  return (ConT colTName', syn : extraDecs ++ lenses)
  where colTName = sanitizeTypeName (prefix <> capitalize1 colName)
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

-- * Default CSV Parsing

-- | Control how row and named column types are generated.
-- The first type argument is @Text@ or @Int@ depending how columns are indexed.
-- The second type argument is a type-level list of the possible column types.
data RowGen (b :: GHC.Type) (a :: [GHC.Type]) =
  RowGen { columnHandler    :: ICSV.RowGenColumnHandler b
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
         , lineReader :: Separator -> Streamly.SerialT IO [Text]
           -- ^ A producer of rows of ’T.Text’ values that were
           -- separated by a 'Separator' value.
         }

-- -- | Shorthand for a 'Proxy' value of 'ColumnUniverse' applied to the
-- -- given type list.
-- colQ :: Name -> Q Exp
-- colQ n = [e| (Proxy :: Proxy (ColumnUniverse $(conT n))) |]

-- | A default 'RowGen'. This instructs the type inference engine to
-- get column names from the data file, use the default column
-- separator (a comma), infer column types from the default 'Columns'
-- set of types, and produce a row type with name @Row@.
rowGen :: FilePath -> RowGen Text CommonColumns
rowGen = RowGen allColumnsAsNamed "" defaultSep "Row" Proxy 1000 . SCSV.streamTokenized'

-- | Like 'rowGen', but will also generate custom data types for
-- 'Categorical' variables with up to 8 distinct variants.
rowGenCat :: FilePath -> RowGen Text CommonColumnsCat
rowGenCat = RowGen allColumnsAsNamed "" defaultSep "Row" Proxy 1000 . SCSV.streamTokenized'

-- | Update or replace the columnHandler in a RowGen
modifyColumnHandler :: (ICSV.RowGenColumnHandler a -> ICSV.RowGenColumnHandler b) -> RowGen a x -> RowGen b x
modifyColumnHandler f rg =
  let newColHandler = f (Frames.Streamly.TH.columnHandler rg)
  in rg { Frames.Streamly.TH.columnHandler = newColHandler }
{-# INLINEABLE modifyColumnHandler #-}

-- | Default Column Handler. Declare one type per column.
-- Use the header to generate names.
allColumnsAsNamed :: ICSV.RowGenColumnHandler Text
allColumnsAsNamed = ICSV.GenUsingHeader Include
{-# INLINEABLE allColumnsAsNamed #-}

-- | Helper for decalring column types from a file with no header.
noHeaderColumnsNumbered' :: ICSV.RowGenColumnHandler Int
noHeaderColumnsNumbered' = ICSV.GenWithoutHeader $ \n -> ICSV.Include $ show n
{-# INLINEABLE noHeaderColumnsNumbered' #-}

-- | Use a given prefix and append the column number to generate column types for a file with no header.
noHeaderColumnsNumbered :: Text -> ICSV.RowGenColumnHandler Int
noHeaderColumnsNumbered prefix = prefixColumns prefix $ noHeaderColumnsNumbered'
{-# INLINEABLE noHeaderColumnsNumbered #-}

-- | Add a prefix to all the generated column type names.
prefixColumns :: Text -> ICSV.RowGenColumnHandler a -> ICSV.RowGenColumnHandler a
prefixColumns p ch = ICSV.modifyColumnStateFunction ch g where
  g f = \x -> case f x of
    ICSV.Exclude -> ICSV.Exclude
    ICSV.Include t -> ICSV.Include $ p <> t
{-# INLINEABLE prefixColumns #-}

-- | Generate all column type names from the header but add a prefix.
prefixAsNamed :: Text -> ICSV.RowGenColumnHandler Text
prefixAsNamed p = prefixColumns p allColumnsAsNamed
{-# INLINEABLE prefixAsNamed #-}

-- | Generate types for only a subset of the columns.
-- Generated 'ParserOptions' will select the correct columns when loading.
columnSubset :: Ord a => Set a -> ICSV.RowGenColumnHandler a -> ICSV.RowGenColumnHandler a
columnSubset s rgch = modifyColumnStateFunction rgch g where
  g f = \x -> if x `Set.member` s then f x else ICSV.Exclude
{-# INLINEABLE columnSubset #-}

-- | Generate Column Type Names for only the headers given in the map.  Use
-- the names given as values in the map rather than those in the header.
-- Generated 'ParserOptions' will select the correct columns when loading.
renamedHeaderSubset :: Map Text Text -> ICSV.RowGenColumnHandler Text
renamedHeaderSubset renamedS = ICSV.GenUsingHeader f where
  f x = case Map.lookup x renamedS of
    Nothing -> Exclude
    Just t -> Include t
{-# INLINEABLE renamedHeaderSubset #-}

-- | Generate Column Type Names for only the given numbered Columns using
-- names given as values in the map.  Can ignore a header row or work without one
-- as set by the first parameter.
-- Generated 'ParserOptions' will select the correct columns when loading.
namedColumnNumberSubset :: Bool -> Map Int Text -> ICSV.RowGenColumnHandler Int
namedColumnNumberSubset hasHeader namedS =
  case hasHeader of
    True -> ICSV.GenIgnoringHeader f
    False -> ICSV.GenWithoutHeader f
  where
    f n = case Map.lookup n namedS of
      Nothing -> ICSV.Exclude
      Just t -> ICSV.Include t
{-# INLINEABLE namedColumnNumberSubset #-}

-- | Use the given names to declare Column Type Names for the
-- first N (= length of the given list of names) columns of the given file.
-- Generated 'ParserOptions' will select the correct columns when loading.
namesGiven :: Bool -> [Text] -> ICSV.RowGenColumnHandler Int
namesGiven hasHeader names = namedColumnNumberSubset hasHeader m
  where
    m = Map.fromList $ zip [0..] names
{-# INLINEABLE namesGiven #-}

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

-- | Inspect no more than this many lines when inferring column types.
prefixSize :: Int
prefixSize = 1000

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
  (colNames, pch) <- runIO $ case columnHandler of
    ICSV.GenUsingHeader f -> do
      allHeaders <- colNamesP (lineReader separator)
      let allColStates = f <$> allHeaders
          cNames = ICSV.colStatesToColNames allColStates
          pch = ICSV.colStatesAndHeadersToParseColHandler allColStates allHeaders
      return (cNames, pch)
    ICSV.GenIgnoringHeader f -> do
      allHeaders <- colNamesP (lineReader separator)
      let allIndexes = fst $ unzip $ zip [0..] allHeaders
          allColStates = f <$> allIndexes
          cNames = ICSV.colStatesToColNames allColStates
      return (cNames, ICSV.ParseIgnoringHeader allColStates)
    ICSV.GenWithoutHeader f -> do
      exampleRow <- colNamesP (lineReader separator)
      let allIndexes = fst $ unzip $ zip [0..] exampleRow
          allColStates = f <$> allIndexes
          cNames = ICSV.colStatesToColNames allColStates
      return (cNames, ICSV.ParseWithoutHeader allColStates)

  let opts = ParserOptions pch separator (RFC4180Quoting '\"')
  let headers = zip colNames (repeat (ConT ''T.Text))
  (colTypes, colDecs) <- second concat . unzip
                         <$> mapM (uncurry mkColDecs) headers
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
            Nothing -> colDec (T.pack tablePrefix) rowTypeName colNm (Right colTy)

-- | Generate a type for a row of a table. This will be something like
-- @Record ["x" :-> a, "y" :-> b, "z" :-> c]@. Additionally generates
-- a type synonym for each column, and a proxy value of that type. If
-- the CSV file has column names \"foo\", \"bar\", and \"baz\", then
-- this will declare @type Foo = "foo" :-> Int@, for example, @foo =
-- rlens \@Foo@, and @foo' = rlens' \@Foo@.
tableTypes' :: forall a b c. (c ~ CoRec ColInfo a, ColumnTypeable c, Monoid c)
            => RowGen b a -> DecsQ
tableTypes' (RowGen {..}) = do
  (headers, pch) <- runIO $ SCSV.readColHeaders columnHandler lineSource :: Q ([(T.Text, c)], ICSV.ParseColumnHandler)
  (colTypes, colDecs) <- (second concat . unzip)
                         <$> mapM (uncurry mkColDecs)
                         (map (second colType) headers)
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
        mkColDecs :: T.Text -> Either (String -> Q [Dec]) Type -> Q (Type, [Dec])
        mkColDecs colNm colTy = do
          let safeName = tablePrefix ++ (T.unpack . sanitizeTypeName $ colNm)
          mColNm <- lookupTypeName safeName
          case mColNm of
            Just n -> pure (ConT n, []) -- Column's type was already defined
            Nothing -> colDec (T.pack tablePrefix) rowTypeName colNm colTy
