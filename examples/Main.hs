{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import qualified DemoPaths as Paths
import qualified Frames
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.Transform as FStreamly
import qualified Frames.Streamly.TH as FStreamly
import qualified Data.Vinyl as V
import qualified Text.Printf as Printf
import qualified Streamly.Prelude as Streamly
import Data.Text (Text) -- for Frames template splicing
import qualified Data.Text as Text

import qualified Control.Foldl as FL
import Data.List (intercalate)

FStreamly.tableTypes "ForestFires" (Paths.thPath Paths.forestFiresPath)
FStreamly.tableTypes' Paths.ffColSubsetRowGen

main :: IO ()
main = do
  forestFiresPath <- Paths.usePath Paths.forestFiresPath
  forestFires :: Frames.Frame ForestFires <- FStreamly.inCoreAoS $ FStreamly.readTableOpt forestFiresParser forestFiresPath
  forestFiresColSubset :: Frames.Frame FFColSubset <- FStreamly.inCoreAoS $ FStreamly.readTableOpt fFColSubsetParser forestFiresPath
  let filterAndMap :: ForestFires -> Maybe (Frames.Record [X, Y, Month, Day, Temp, Wind])
      filterAndMap r = if Frames.rgetField @Day r == "fri" then (Just $ Frames.rcast r) else Nothing
      forestFires' = FStreamly.mapMaybe filterAndMap forestFires
      forestFiresColSubset' = FStreamly.filter  (\r -> Frames.rgetField @Day r == "fri") forestFiresColSubset
      formatRow = FStreamly.formatWithShow
              V.:& FStreamly.formatWithShow
              V.:& FStreamly.formatTextAsIs
              V.:& FStreamly.formatTextAsIs
              V.:& FStreamly.liftFieldFormatter (Text.pack . Printf.printf "%.1f")
              V.:& FStreamly.liftFieldFormatter (Text.pack . Printf.printf "%.1f")
              V.:& V.RNil
      csvTextStream = FStreamly.streamSV' formatRow "," $ Streamly.fromFoldable forestFires'
      csvTextStreamCS = FStreamly.streamSV' formatRow "," $ Streamly.fromFoldable forestFiresColSubset'
  putStrLn $ intercalate "\n" $ fmap show $ FL.fold FL.list forestFiresColSubset'
  Streamly.toList csvTextStream >>= putStrLn . Text.unpack . Text.intercalate "\n"
  FStreamly.writeLines "exampleOut.csv" csvTextStream
  FStreamly.writeLines "exampleOutCS.csv" csvTextStreamCS
