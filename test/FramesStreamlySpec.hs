{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
module FramesStreamlySpec where

import SpecHelper

import Data.Text (Text)
import qualified DemoPaths as Paths
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames
import qualified Control.Foldl as Foldl

numRecs :: Foldable f => f a -> Int
numRecs = Foldl.fold Foldl.length

FStreamly.tableTypes "ForestFires" (Paths.thPath Paths.forestFiresPath)
FStreamly.tableTypes' Paths.ffColSubsetRowGen
FStreamly.tableTypes' Paths.ffNoHeaderRowGen
FStreamly.tableTypes' Paths.ffIgnoreHeaderRowGen
FStreamly.tableTypes' Paths.ffIgnoreHeaderChooseNamesRowGen

spec :: Spec
spec = do
  forestFiresPath <- runIO $ Paths.usePath Paths.forestFiresPath
  forestFiresNoHeaderPath <- runIO $ Paths.usePath Paths.forestFiresNoHeaderPath
  forestFires :: Frames.Frame ForestFires <- runIO $ FStreamly.inCoreAoS $ FStreamly.readTableOpt forestFiresParser forestFiresPath
  forestFiresColSubset :: Frames.Frame FFColSubset <- runIO $ FStreamly.inCoreAoS $ FStreamly.readTableOpt fFColSubsetParser forestFiresPath
  forestFiresNoHeader :: Frames.Frame FFNoHeader <- runIO $ FStreamly.inCoreAoS $ FStreamly.readTableOpt fFNoHeaderParser forestFiresNoHeaderPath
  forestFiresIgnoreHeader :: Frames.Frame FFIgnoreHeader <- runIO $ FStreamly.inCoreAoS $ FStreamly.readTableOpt fFIgnoreHeaderParser forestFiresPath
  forestFiresIgnoreHeaderChooseNames :: Frames.Frame FFIgnoreHeaderChooseNames <-
    runIO $ FStreamly.inCoreAoS $ FStreamly.readTableOpt fFIgnoreHeaderChooseNamesParser forestFiresPath
  describe "Frames.Streamly" $ do
    context "Can generate types and load the corresponding data a variety of ways" $ do
      it "load frames and insure all have same number of rows (AllCols  vs. ColSubset)" $
        numRecs forestFires == numRecs forestFiresColSubset
      it "load frames and insure all have same number of rows (AllCols  vs. NoHeader)" $
        numRecs forestFires == numRecs forestFiresNoHeader
      it "load frames and insure all have same number of rows (AllCols  vs. IgnoreHeader)" $
        numRecs forestFires == numRecs forestFiresIgnoreHeader
      it "load frames and insure all have same number of rows (AllCols  vs. IgnoreHeaderChooseNames)" $
        numRecs forestFires == numRecs forestFiresIgnoreHeaderChooseNames
      it "generate types from each header and rcast to drop columns or load only that column subset.  Those should be the same" $
        let rcasted :: Frames.FrameRec [X, Y, Month, Day, Temp, Wind] = fmap Frames.rcast forestFires
        in rcasted == forestFiresColSubset
      it "generate types from column numbers rather than headers.  Also from column numbers when there is no header.  Those should be the same." $
        forestFiresNoHeader == forestFiresIgnoreHeader
      it "generate named types for columns chosen by col number.  In this case, should match col subset tested above" $
        forestFiresIgnoreHeaderChooseNames == forestFiresColSubset
