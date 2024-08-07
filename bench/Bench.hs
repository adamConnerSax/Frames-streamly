{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Main where

import Criterion.Main
import Paths

import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.LoadInCore as FStreamly
import qualified Frames.Streamly.Transform as FStreamly
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.ColumnUniverse as FStreamly
import qualified Frames.Streamly.Streaming.Class as Streaming
import qualified Frames.Streamly.Streaming.Pipes as StreamP
import qualified Frames.Streamly.Streaming.Streamly as StreamS

import qualified Frames hiding (inCoreAoS)
import Frames.CSV as Frames
import Frames.TH as Frames
import Frames.InCore as Frames

import qualified Data.Vinyl as V
import qualified Pipes
import qualified Pipes.Prelude as Pipes

import GHC.TypeLits (KnownSymbol)
import qualified Control.Foldl as FL
import Control.Monad.IO.Class (liftIO)

liftIO ffNewRowGenIO >>= FStreamly.tableTypes'
liftIO ffRowGenIO >>= Frames.tableTypes'
liftIO (ffColSubsetRowGenIO "forestFires.csv") >>= FStreamly.tableTypes'
liftIO (ffInferTypedSubsetRGIO  "forestFires.csv") >>= FStreamly.tableTypes'


loadAndCountLines :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCountLines n = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  Streaming.runSafe @s $ Streaming.sLength $ Streaming.sReadTextLines @s @IO forestFiresPath

{-
loadTokenizeRawAndCountCells :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadTokenizeRawAndCountCells n = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  Streaming.runSafe @s $ Streaming.sFolder (+) 0 $ Streaming.sMap length $ Streaming.sTokenizedRaw @s @IO FStreamly.defaultSep forestFiresPath
-}
loadTokenizeAndCountCells :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadTokenizeAndCountCells n = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  Streaming.runSafe @s $ Streaming.sFolder (+) 0 $ Streaming.sMap length $ Streaming.sTokenized @s @IO FStreamly.defaultSep FStreamly.NoQuoting  forestFiresPath

loadAndCountRecs :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCountRecs n = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  Streaming.runSafe @s $ Streaming.sLength $ FStreamly.readTableOpt @(Frames.RecordColumns FFNew) @s @IO fFNewParser forestFiresPath


loadAndCountFrame :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCountFrame n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFNew <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFNewParser forestFiresPath
  return $ fLength forestFires

loadAndCountFrameF :: Int -> IO Int
loadAndCountFrameF n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FF <- Frames.runSafeT $ Frames.inCoreAoS $ Frames.readTableOpt fFParser forestFiresPath
  return $ fLength forestFires


loadAndTransform :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndTransform n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires' :: Frames.FrameRec [MthC, DayC, X, Y, AX]  <-
    Streaming.runSafe @s
    $ FStreamly.inCoreAoS
    $ Streaming.sMapMaybe (either (const Nothing) Just . transform)
    $ FStreamly.readTableOpt @_ @s @IO fFNewParser forestFiresPath
  return $ fLength forestFires'

loadInCore :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadInCore n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires' :: Frames.FrameRec [MthC, DayC, X, Y, AX]  <-
    Streaming.runSafe @s $ FStreamly.loadInCore @s @IO fFNewParser forestFiresPath (either (const Nothing) Just . transform)
  return $ fLength forestFires'

{-
loadInCore2 :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadInCore2 n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires' :: Frames.FrameRec [MthC, DayC, X, Y, AX]  <-
    Streaming.runSafe @s $ FStreamly.loadInCore2 @s @IO fFNewParser forestFiresPath (either (const Nothing) Just . transform)
  return $ fLength forestFires'
-}

loadAndTransformF :: Int -> IO Int
loadAndTransformF n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires' :: Frames.FrameRec [MthC, DayC, X, Y, AX]  <-
    Frames.runSafeT
    $ Frames.inCoreAoS
    $ Frames.readTableOpt fFParser forestFiresPath Pipes.>-> Pipes.mapMaybe (either (const Nothing) Just . transform)
  return $ fLength forestFires'

loadSubset :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadSubset n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFColSubset <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFColSubsetParser forestFiresPath
  return $ fLength forestFires


rcastSubset :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
rcastSubset n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFColSubset <- Streaming.runSafe @s
                                             $ FStreamly.inCoreAoS
                                             $ Streaming.sMap (Frames.rcast @(Frames.RecordColumns FFColSubset))
                                             $ FStreamly.readTableOpt @(Frames.RecordColumns FF) @s @IO fFNewParser forestFiresPath
  return $ fLength forestFires

loadTypedSubset :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadTypedSubset n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFInferTyped <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFInferTypedParser forestFiresPath
  return $ fLength forestFires


loadSubsetAndRetype :: forall s. Streaming.StreamFunctionsIO s IO
                    => (Frames.Record [X, Y, Month, Day, Wind] -> Maybe (Frames.Record [X, Y, PMonth, PDay, Wind]))
                    -> Int
                    -> IO Int
loadSubsetAndRetype f n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.FrameRec [X,Y,PMonth, PDay, Wind] <- Streaming.runSafe @s
                                                             $ FStreamly.inCoreAoS
                                                             $ Streaming.sMapMaybe f
                                                             $ FStreamly.readTableOpt @_ @s @IO fFColSubsetParser forestFiresPath
  return $ fLength forestFires

retype1 :: Frames.Record [X, Y, Month, Day, Wind] -> Maybe (Frames.Record [X, Y, PMonth, PDay, Wind])
retype1 r = do
  let x = Frames.rgetField @X r
      y = Frames.rgetField @Y r
      wind = Frames.rgetField @Wind r
  pmth <- either (const Nothing) Just $ parseMth $ Frames.rgetField @Month r
  pday <- either (const Nothing) Just $ parseDayOfWeek $ Frames.rgetField @Day r
  return $ x Frames.&: y Frames.&: pmth Frames.&: pday Frames.&: wind Frames.&: V.RNil

retype2 :: Frames.Record [X, Y, Month, Day, Wind] -> Maybe (Frames.Record [X, Y, PMonth, PDay, Wind])
retype2 r = do
  pmth <- either (const Nothing) Just $ parseMth $ Frames.rgetField @Month r
  pday <- either (const Nothing) Just $ parseDayOfWeek $ Frames.rgetField @Day r
  let mkMthDay :: Frames.Record [PMonth, PDay]
      mkMthDay = pmth Frames.&: pday Frames.&: V.RNil
  return $ Frames.rcast $ r V.<+> mkMthDay

inferTypes :: forall s b a.(Streaming.StreamFunctionsIO s IO
                           , Show (FStreamly.ColumnIdType b)
                           , V.RFoldMap a
                           , V.RMap a
                           , V.RApply a)
           => FStreamly.RowGen s b a -> IO ()
inferTypes FStreamly.RowGen{..} = do
  x ::  ([FStreamly.ColTypeInfo (FStreamly.ColType a)], FStreamly.ParseColumnSelector) <-
    Streaming.runSafe @s
    $ FStreamly.readColHeaders columnParsers genColumnSelector $ lineReader separator
  return ()

inferTypesF :: FilePath -> IO ()
inferTypesF fp = do
  let lr = Frames.produceTokens fp (Frames.columnSeparator fFParser)
  _ <- Frames.runSafeT $ readColHeaders @(Frames.CoRec Frames.ColInfo Frames.CommonColumns) fFParser lr
  return ()


concatFrames :: forall s. Streaming.StreamFunctionsIO s IO
             => (forall rs . (FStreamly.RecVec rs) => [Frames.FrameRec rs] -> IO (Frames.FrameRec rs))
             -> Int -> Int -> IO Double
concatFrames fConcat numCopies numRows = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show numRows <> ".csv"
  forestFires :: Frames.Frame FFNew <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFNewParser forestFiresPath
  let !manyFrames = replicate numCopies forestFires
  newFrame <- fConcat manyFrames
  pure $ FL.fold (FL.premap (Frames.rgetField @FFMC) FL.sum) newFrame

frameConcatMonoid :: (FStreamly.RecVec rs, Foldable f, Functor f) => f (Frames.FrameRec rs) -> Frames.FrameRec rs
frameConcatMonoid x = mconcat $ toList x

frameConcatToFrame :: (FStreamly.RecVec rs, Foldable f, Functor f) => f (Frames.FrameRec rs) -> Frames.FrameRec rs
frameConcatToFrame x =  if length x < 500
                        then mconcat $ toList x
                        else Frames.toFrame $ concatMap toList x

benchConcat includeMonoid n m = bgroup ("concat " <> show n <> "x" <> show m) $
                                ((if includeMonoid
                                   then [bench "monoid" $ nfIO (concatFrames @(FStreamly.DefaultStream)  (pure . frameConcatMonoid) n m)]
                                   else [])
                                  <>
                                  [
                                    bench "toFrame" $ nfIO (concatFrames @(FStreamly.DefaultStream) (pure . frameConcatToFrame) n m)
                                  , bench "streamly" $ nfIO (concatFrames @(FStreamly.DefaultStream) (pure . FStreamly.frameConcat) n m)
                                  ]
                                )


main :: IO ()
main = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let fp5000 = forestFiresPathPrefix <> "5000.csv"
  defaultMain [
{-
  bgroup "loadAndCountRecs (500)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 500)
                              , bench "Streamly" $ nfIO (loadAndCountRecs @(FStreamly.DefaultStream) 500)
                              , bench "Frames" $ nfIO (loadAndCountRecsF 500)
                              ]
  , bgroup "loadAndTransform (500)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 500)
                                    , bench "Streamly" $ nfIO (loadAndTransform @(FStreamly.DefaultStream) 500)
                                    , bench "Frames" $ nfIO (loadAndTransformF 500)
                              ]
-}
    benchConcat True 100 500
    , benchConcat True 500 500
    , benchConcat True 1000 500
    , benchConcat False 2000 500



{-
    , bgroup "inference (1000/5000)" [ bench "Pipes" $ nfIO (inferTypes $ ffNewRowGenP fp5000)
                                   , bench "Streamly" $ nfIO (inferTypes $ ffNewRowGenS fp5000)
                                   , bench "Frames" $ nfIO $ inferTypesF fp5000
                                   , bench "Pipes/subset" $ nfIO $ inferTypes $ ffColSubsetRowGen "forestFires5000.csv"
                                   ]

    , bgroup "loadAndCountLines (5000)" [ bench "Pipes" $ nfIO $ loadAndCountLines @StreamP.PipeStream 5000
                                        , bench "Streamly" $ nfIO $ loadAndCountLines @(FStreamly.DefaultStream) 5000]
    , bgroup "loadTokenizeAndCountCells (5000)" [ bench "Pipes" $ nfIO $ loadTokenizeAndCountCells @StreamP.PipeStream 5000
                                                , bench "Streamly" $ nfIO $ loadTokenizeAndCountCells @(FStreamly.DefaultStream) 5000]

    , bgroup "loadAndCountRecs (5000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 5000)
                                   , bench "Streamly" $ nfIO (loadAndCountRecs @(FStreamly.DefaultStream) 5000)
                                   ]
    , bgroup "loadAndCountFrame (5000)" [ bench "Pipes" $ nfIO (loadAndCountFrame @StreamP.PipeStream 5000)
                                        , bench "Streamly" $ nfIO (loadAndCountFrame @(FStreamly.DefaultStream) 5000)
                                        , bench "Frames" $ nfIO (loadAndCountFrameF 5000)
                                        ]
    , bgroup "loadAndTransform (5000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 5000)
                                       , bench "Streamly" $ nfIO (loadAndTransform @(FStreamly.DefaultStream) 5000)
                                       , bench "Frames" $ nfIO (loadAndTransformF 5000)
                                       ]
    , bgroup "loadInCore (5000)" [ bench "Pipes" $ nfIO (loadInCore @StreamP.PipeStream 5000)
                                 , bench "Streamly" $ nfIO (loadInCore @(FStreamly.DefaultStream) 5000)
                                 ]
-}
{-
    , bgroup "loadInCore2 (5000)" [ bench "Pipes" $ nfIO (loadInCore2 @StreamP.PipeStream 5000)
                                  , bench "Streamly" $ nfIO (loadInCore2 @(FStreamly.DefaultStream) 5000)
                                  ]
  , bgroup "loadAndCountRecs (50000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 50000)
                                  , bench "Streamly" $ nfIO (loadAndCountRecs @(FStreamly.DefaultStream) 50000)
                                  , bench "Frames" $ nfIO (loadAndCountRecsF 50000)
                                  ]
  , bgroup "loadAndTransform (50000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 50000)
                                      , bench "Streamly" $ nfIO (loadAndTransform @(FStreamly.DefaultStream) 50000)
                                      , bench "Frames" $ nfIO (loadAndTransformF 50000)
                                    ]

    , bgroup "colSubset (5000)" [ bench "Pipes/load-subset" $ nfIO (loadSubset @StreamP.PipeStream 5000)
                                , bench "Pipes/rcast" $ nfIO (rcastSubset @StreamP.PipeStream 5000)
                                , bench "Streamly/load-subset" $ nfIO (loadSubset @(FStreamly.DefaultStream) 5000)
                                , bench "Streamly/rcast" $ nfIO (rcastSubset @(FStreamly.DefaultStream) 5000)
                                ]

    , bgroup "custom-parsing (5000)" [ bench "Pipes/load-parsed" $ nfIO (loadTypedSubset @StreamP.PipeStream 5000)
                                     , bench "Pipes/parseAfter v1" $ nfIO (loadSubsetAndRetype @StreamP.PipeStream retype1 5000)
                                     , bench "Pipes/parseAfter v2" $ nfIO (loadSubsetAndRetype @StreamP.PipeStream retype2 5000)
                                     , bench "Streamly/load-parsed" $ nfIO (loadTypedSubset @(FStreamly.DefaultStream) 5000)
                                     , bench "Streamly/parseAfter v1" $ nfIO (loadSubsetAndRetype @(FStreamly.DefaultStream) retype1 5000)
                                     , bench "Streamly/parseAfter v2" $ nfIO (loadSubsetAndRetype @(FStreamly.DefaultStream) retype2 5000)
                                     ]


  , bgroup "loadAndCountRecs (500000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 500000)
                                   , bench "Streamly" $ nfIO (loadAndCountRecs @(FStreamly.DefaultStream) 500000)
                                   , bench "Frames" $ nfIO (loadAndCountRecsF 500000)
                                   ]
-}
    ]

-- | Create a record with one field from a value.  Use a TypeApplication to choose the field.
recordSingleton :: forall af s a. (KnownSymbol s, af ~ '(s,a)) => a -> Frames.Record '[af]
recordSingleton a = a Frames.&: V.RNil
{-# INLINE recordSingleton #-}

type AX = "AX" Frames.:-> Double

transform :: Frames.Record [X,Y,Month,Day,Temp,Wind] -> Either Text (Frames.Record [MthC, DayC, X, Y, AX])
transform r = do
  let ax = recordSingleton @AX $ Frames.rgetField @Wind r + Frames.rgetField @Temp r
  day <- fmap (recordSingleton @DayC) . parseDayOfWeek $ Frames.rgetField @Day r
  mth <- fmap (recordSingleton @MthC) . parseMth $  Frames.rgetField @Month r
  return $ Frames.rcast $ r V.<+> ax V.<+> day V.<+> mth
{-# INLINEABLE transform #-}
