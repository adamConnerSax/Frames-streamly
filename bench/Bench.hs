{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Main where

import Criterion.Main
import Paths

import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.TH as FStreamly
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

loadAndCount :: forall s. Streaming.StreamFunctionsIO s IO => Int -> IO Int
loadAndCount n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFNew <- Streaming.runSafe @s $ FStreamly.inCoreAoS $ FStreamly.readTableOpt @_ @s @IO fFNewParser forestFiresPath
  return $ fLength forestFires

loadAndCountF :: Int -> IO Int
loadAndCountF n = do
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



main = defaultMain [
  bgroup "loadAndCount (500)" [ bench "Pipes" $ nfIO (loadAndCount @StreamP.PipeStream 500)
                              , bench "Streamly" $ nfIO (loadAndCount @(StreamS.StreamlyStream StreamS.SerialT) 500)
                              , bench "Frames" $ nfIO (loadAndCountF 500)
                              ]
  , bgroup "loadAndTransform (500)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 500)
                                    , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 500)
                                    , bench "Frames" $ nfIO (loadAndTransformF 500)
                              ]
  , bgroup "loadAndCount (5000)" [ bench "Pipes" $ nfIO (loadAndCount @StreamP.PipeStream 5000)
                                 , bench "Streamly" $ nfIO (loadAndCount @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                 , bench "Frames" $ nfIO (loadAndCountF 5000)
                                 ]
  , bgroup "loadAndTransform (5000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 5000)
                                     , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 5000)
                                     , bench "Frames" $ nfIO (loadAndTransformF 5000)
                              ]
  , bgroup "loadAndCount (50000)" [ bench "Pipes" $ nfIO (loadAndCount @StreamP.PipeStream 50000)
                                  , bench "Streamly" $ nfIO (loadAndCount @(StreamS.StreamlyStream StreamS.SerialT) 50000)
                                  , bench "Frames" $ nfIO (loadAndCountF 50000)
                                  ]
  , bgroup "loadAndTransform (50000)" [ bench "Pipes" $ nfIO (loadAndTransform @StreamP.PipeStream 50000)
                                      , bench "Streamly" $ nfIO (loadAndTransform @(StreamS.StreamlyStream StreamS.SerialT) 50000)
                                      , bench "Frames" $ nfIO (loadAndTransformF 50000)
                                    ]
{-
  , bgroup "loadAndCount (500000)" [ bench "Pipes" $ nfIO (loadAndCount @StreamP.PipeStream 500000)
                                   , bench "Streamly" $ nfIO (loadAndCount @(StreamS.StreamlyStream StreamS.SerialT) 500000)
                                   , bench "Frames" $ nfIO (loadAndCountF 500000)
                                   ]
-}
  ]

-- | Create a record with one field from a value.  Use a TypeApplication to choose the field.
recordSingleton :: forall af s a. (KnownSymbol s, af ~ '(s,a)) => a -> Frames.Record '[af]
recordSingleton a = a Frames.&: V.RNil

type AX = "AX" Frames.:-> Double

transform :: Frames.Record [X,Y,Month,Day,Temp,Wind] -> Either Text (Frames.Record [MthC, DayC, X, Y, AX])
transform r = do
  let ax = recordSingleton @AX $ Frames.rgetField @Wind r + Frames.rgetField @Temp r
  day <- fmap (recordSingleton @DayC) . parseDayOfWeek $ Frames.rgetField @Day r
  mth <- fmap (recordSingleton @MthC) . parseMth $  Frames.rgetField @Month r
  return $ Frames.rcast $ r V.<+> ax V.<+> day V.<+> mth
