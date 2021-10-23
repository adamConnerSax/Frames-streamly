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

loadAndCount :: Stream.StreamFunctionsWithIO s IO -> Int -> IO Int
loadAndCount sfWIO n = do
  let sf = Stream.streamFunctions sfWIO
      fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FFNew <- FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFNewParser forestFiresPath
  return $ fLength forestFires

loadAndCountF :: Int -> IO Int
loadAndCountF n = do
  let fLength = FL.fold FL.length
  forestFiresPathPrefix <- Paths.usePath Paths.forestFiresPrefix
  let forestFiresPath = forestFiresPathPrefix <> show n <> ".csv"
  forestFires :: Frames.Frame FF <- Frames.runSafeT $ Frames.inCoreAoS $ Frames.readTableOpt fFParser forestFiresPath
  return $ fLength forestFires


main = defaultMain [
  bgroup "loadAndCount (500)" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO 500)
                              , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO 500)
                              , bench "Frames" $ nfIO (loadAndCountF 500)
                              ]
  , bgroup "loadAndCount (5000)" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO 5000)
                                 , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO 5000)
                                 , bench "Frames" $ nfIO (loadAndCountF 5000)
                                 ]

  , bgroup "loadAndCount (50000)" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO 50000)
                                  , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO 50000)
                                  , bench "Frames" $ nfIO (loadAndCountF 50000)
                                  ]

  , bgroup "loadAndCount (500000)" [ bench "Pipes" $ nfIO (loadAndCount StreamP.pipesFunctionsWithIO 500000)
                                   , bench "Streamly" $ nfIO (loadAndCount StreamS.streamlyFunctionsWithIO 500000)
                                   , bench "Frames" $ nfIO (loadAndCountF 500000)
                                   ]
  ]
