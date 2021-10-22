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

import qualified Frames
import Frames.CSV as Frames
import Frames.TH as Frames
import Frames.InCore as Frames

import qualified Control.Foldl as FL

FStreamly.tableTypes' ffRowGen

loadAndCount :: Stream.StreamFunctionsWithIO s IO -> IO Int
loadAndCount sfWIO = do
  let sf = Stream.streamFunctions sfWIO
      fLength = FL.fold FL.length
  forestFiresPath <- Paths.usePath Paths.forestFiresPath
  forestFires :: Frames.Frame FF <- FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFParser forestFiresPath
  return $ fLength forestFires

main = defaultMain [
  bgroup "loadAndCount" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO)
                        , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO)
                        ]

  ]
