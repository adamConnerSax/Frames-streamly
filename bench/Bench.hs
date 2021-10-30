{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PartialTypeSignatures #-}
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

FStreamly.tableTypes' ffNewRowGen
Frames.tableTypes' ffRowGen
FStreamly.tableTypes' (ffColSubsetRowGen "forestFires.csv")
FStreamly.tableTypes' (ffInferTypedSubsetRG  "forestFires.csv")


loadAndCountLines :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCountLines n = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  Streaming.runSafe @s $ Streaming.sLength $ Streaming.sReadTextLines @s @IO forestFiresPath

loadAndCountRecs :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCountRecs n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFNew <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFNewParser forestFiresPath
  return $ fLength forestFires

loadAndCountRecsF :: Int -> IO Int
loadAndCountRecsF n = do
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

loadInCore2 :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadInCore2 n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires' :: Frames.FrameRec [MthC, DayC, X, Y, AX]  <-
    Streaming.runSafe @s $ FStreamly.loadInCore2 @s @IO fFNewParser forestFiresPath (either (const Nothing) Just . transform)
  return $ fLength forestFires'


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

main :: IO ()
main = do
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let fp5000 = forestFiresPathPrefix <> "5000.csv"
  defaultMain [
{-
  bgroup "loadAndCountRecs (500)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 500)
                              , bench "Streamly" $ nfIO (loadAndCountRecs @(StreamS.StreamlyStream StreamS.SerialT) 500)
                              , bench "Frames" $ nfIO (loadAndCountRecsF 500)
                              ]
  , bgroup "loadAndTransform (500)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 500)
                                    , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 500)
                                    , bench "Frames" $ nfIO (loadAndTransformF 500)
                              ]

-}

    bgroup "inference (1000/5000)" [ bench "Pipes" $ nfIO (inferTypes $ ffNewRowGenP fp5000)
                                   , bench "Streamly" $ nfIO (inferTypes $ ffNewRowGenS fp5000)
                                   , bench "Frames" $ nfIO $ inferTypesF fp5000
                                   , bench "Pipes/subset" $ nfIO $ inferTypes $ ffColSubsetRowGen "forestFires5000.csv"
                                   ]

    , bgroup "loadAndCountLines (5000)" [ bench "Pipes" $ nfIO $ loadAndCountLines @StreamP.PipeStream 5000
                                        , bench "Streamly" $ nfIO $ loadAndCountLines @(StreamS.StreamlyStream StreamS.SerialT) 5000]

    , bgroup "loadAndCountRecs (5000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 5000)
                                   , bench "Streamly" $ nfIO (loadAndCountRecs @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                   , bench "Frames" $ nfIO (loadAndCountRecsF 5000)
                                   ]
    , bgroup "loadAndTransform (5000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 5000)
                                       , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                       , bench "Frames" $ nfIO (loadAndTransformF 5000)
                                       ]
    , bgroup "loadInCore (5000)" [ bench "Pipes" $ nfIO (loadInCore @StreamP.PipeStream 5000)
                                 , bench "Streamly" $ nfIO (loadInCore @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                 ]
    , bgroup "loadInCore2 (5000)" [ bench "Pipes" $ nfIO (loadInCore2 @StreamP.PipeStream 5000)
                                  , bench "Streamly" $ nfIO (loadInCore2 @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                  ]
{-
  , bgroup "loadAndCountRecs (50000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 50000)
                                  , bench "Streamly" $ nfIO (loadAndCountRecs @(StreamS.StreamlyStream StreamS.SerialT) 50000)
                                  , bench "Frames" $ nfIO (loadAndCountRecsF 50000)
                                  ]
  , bgroup "loadAndTransform (50000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 50000)
                                      , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 50000)
                                      , bench "Frames" $ nfIO (loadAndTransformF 50000)
                                    ]
-}
    , bgroup "colSubset (5000)" [ bench "Pipes/load-subset" $ nfIO (loadSubset @StreamP.PipeStream 5000)
                                , bench "Pipes/rcast" $ nfIO (rcastSubset @StreamP.PipeStream 5000)
                                , bench "Streamly/load-subset" $ nfIO (loadSubset @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                , bench "Streamly/rcast" $ nfIO (rcastSubset @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                ]

    , bgroup "custom-parsing (5000)" [ bench "Pipes/load-parsed" $ nfIO (loadTypedSubset @StreamP.PipeStream 5000)
                                     , bench "Pipes/parseAfter v1" $ nfIO (loadSubsetAndRetype @StreamP.PipeStream retype1 5000)
                                     , bench "Pipes/parseAfter v2" $ nfIO (loadSubsetAndRetype @StreamP.PipeStream retype2 5000)
                                     , bench "Streamly/load-parsed" $ nfIO (loadTypedSubset @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                     , bench "Streamly/parseAfter v1" $ nfIO (loadSubsetAndRetype @(StreamS.StreamlyStream StreamS.SerialT) retype1 5000)
                                     , bench "Streamly/parseAfter v2" $ nfIO (loadSubsetAndRetype @(StreamS.StreamlyStream StreamS.SerialT) retype2 5000)
                                     ]


  {-
  , bgroup "loadAndCountRecs (500000)" [ bench "Pipes" $ nfIO (loadAndCountRecs @StreamP.PipeStream 500000)
                                   , bench "Streamly" $ nfIO (loadAndCountRecs @(StreamS.StreamlyStream StreamS.SerialT) 500000)
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
