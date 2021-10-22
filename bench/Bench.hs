{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import Criterion.Main
import Paths

import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.Streaming.Interface as Stream
import qualified Frames.Streamly.Streaming.Pipes as StreamP
import qualified Frames.Streamly.Streaming.Streamly as StreamS

import qualified Frames hiding (inCoreAoS)
import Frames.CSV as Frames
import Frames.TH as Frames
import Frames.InCore as Frames

import qualified Control.Foldl as FL

FStreamly.tableTypes' ffNewRowGen
Frames.tableTypes' ffRowGen

loadAndCount :: Stream.StreamFunctionsWithIO s IO -> IO Int
loadAndCount sfWIO = do
  let sf = Stream.streamFunctions sfWIO
      fLength = FL.fold FL.length
  forestFiresPath <- Paths.usePath Paths.forestFiresPath
  forestFires :: Frames.Frame FFNew <- FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFNewParser forestFiresPath
  return $ fLength forestFires

loadAndCountF :: IO Int
loadAndCountF = do
  let fLength = FL.fold FL.length
  forestFiresPath <- Paths.usePath Paths.forestFiresPath
  forestFires :: Frames.Frame FF <- Frames.runSafeT $ Frames.inCoreAoS $ Frames.readTableOpt fFParser forestFiresPath
  return $ fLength forestFires


main = defaultMain [
  bgroup "loadAndCount" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO)
                        , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO)
                        , bench "Frames" $ nfIO loadAndCountF
                        ]

  ]
