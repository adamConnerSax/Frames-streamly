{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE QuantifiedConstraints #-}
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
import qualified Frames.Streamly.Streaming.Interface as Streaming
import qualified Frames.Streamly.Streaming.Pipes as Streaming
import qualified Frames.Streamly.Streaming.Streamly as Streaming
import qualified Frames
import qualified Control.Foldl as Foldl

numRecs :: Foldable f => f a -> Int
numRecs = Foldl.fold Foldl.length

FStreamly.tableTypes "ForestFires" (Paths.thPath Paths.forestFiresPath)
FStreamly.tableTypes' Paths.ffColSubsetRowGen
FStreamly.tableTypes' Paths.ffRenameDayRowGen
FStreamly.tableTypes' Paths.ffNoHeaderRowGen
FStreamly.tableTypes' Paths.ffIgnoreHeaderRowGen
FStreamly.tableTypes' Paths.ffIgnoreHeaderChooseNamesRowGen

-- TODO
-- 1. Test renameSome
-- 2. Test different files with shared chosen columnNames

emptyStreamSelector :: Selector FStreamly.FramesCSVException
emptyStreamSelector FStreamly.EmptyStreamException = True
emptyStreamSelector _ = False

missingHeadersSelector :: Selector FStreamly.FramesCSVException
missingHeadersSelector (FStreamly.MissingHeadersException _) = True
missingHeadersSelector _ = False

badHeaderSelector :: Selector FStreamly.FramesCSVException
badHeaderSelector (FStreamly.BadHeaderException _) = True
badHeaderSelector _ = False

wrongNumberColumnsSelector :: Selector FStreamly.FramesCSVException
wrongNumberColumnsSelector (FStreamly.WrongNumberColumnsException _) = True
wrongNumberColumnsSelector _ = False

spec :: Spec
spec = do
  let specF :: String -> Streaming.StreamFunctionsWithIO s IO -> Spec
      specF desc sfWIO = do
        let sf = Streaming.streamFunctions sfWIO
        forestFiresPath <- runIO $ Paths.usePath Paths.forestFiresPath
        forestFiresNoHeaderPath <- runIO $ Paths.usePath Paths.forestFiresNoHeaderPath
        forestFiresFewerColsPath <- runIO $ Paths.usePath Paths.forestFiresFewerColsPath

        forestFires :: Frames.Frame ForestFires <- runIO $ FStreamly.inCoreAoS sf  $ FStreamly.readTableOpt sfWIO forestFiresParser forestFiresPath
        forestFiresColSubset :: Frames.Frame FFColSubset <- runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFColSubsetParser forestFiresPath
        forestFiresFewerCols :: Frames.Frame FFColSubset <- runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFColSubsetParser forestFiresFewerColsPath
        forestFiresRenameDay :: Frames.Frame FFRenameDay <- runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFColSubsetParser forestFiresPath
        forestFiresNoHeader :: Frames.Frame FFNoHeader <- runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFNoHeaderParser forestFiresNoHeaderPath
        forestFiresIgnoreHeader :: Frames.Frame FFIgnoreHeader <- runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFIgnoreHeaderParser forestFiresPath
        forestFiresIgnoreHeaderChooseNames :: Frames.Frame FFIgnoreHeaderChooseNames <-
          runIO $ FStreamly.inCoreAoS sf $ FStreamly.readTableOpt sfWIO fFIgnoreHeaderChooseNamesParser forestFiresPath
        describe desc $ do
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
            it "parse a file with different columsn as long as the required subset is present. This should be the same as the full file" $
              forestFiresColSubset == forestFiresFewerCols
            it "generate types from column numbers rather than headers.  Also from column numbers when there is no header.  Those should be the same." $
              forestFiresNoHeader == forestFiresIgnoreHeader
            it "generate named types for columns chosen by col number.  In this case, should match col subset after renaming Day to DayOfWeek" $
              forestFiresIgnoreHeaderChooseNames == forestFiresRenameDay
            it "throw an exception when parsing (ignoring or absent a header line) a file with the wrong number of columns" $ do
              (FStreamly.inCoreAoS sf $ FStreamly.readTableOpt @(Frames.RecordColumns ForestFires) sfWIO fFIgnoreHeaderParser forestFiresFewerColsPath) `shouldThrow` wrongNumberColumnsSelector
  specF "Streamly backend" Streaming.streamlyFunctionsWithIO
  specF "Pipes backend" Streaming.pipesFunctionsWithIO
