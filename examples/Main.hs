{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import qualified DemoPaths as Paths
import qualified Frames
--import Frames.Streamly.Categorical ()
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.Transform as FStreamly
import qualified Frames.Streamly.TH as FStreamly
import Frames.Streamly.ColumnTypeable ()
import qualified Data.Vinyl as V
import qualified Data.Vinyl.Functor as V
import qualified Text.Printf as Printf
import qualified Streamly.Prelude as Streamly
import Data.Text (Text) -- for Frames template splicing
import qualified Data.Text as Text

import qualified Control.Foldl as FL
import Data.List (intercalate)
import DemoPaths (cesPath)

FStreamly.tableTypes "ForestFires" (Paths.thPath Paths.forestFiresPath)
FStreamly.tableTypes' Paths.ffColSubsetRowGen
FStreamly.tableTypes' Paths.ffColSubsetRowGenCat
--FStreamly.tableTypes' Paths.cesRowGen
FStreamly.tableTypes' Paths.ffInferMaybeRowGen
FStreamly.tableTypes' Paths.ffInferMaybeRowGenCat

FStreamly.declareColumnType "MDay" [t|Maybe Text|]
FStreamly.declareColumnType "MWind" [t|Maybe Int|]





type FFMRow = [X,Y,Month, MDay, Temp, MWind]

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
--      csvTextStreamCS = FStreamly.streamSV' formatRow "," $ Streamly.fromFoldable forestFiresColSubset'

--  putStrLn $ intercalate "\n" $ fmap show $ FL.fold FL.list forestFiresColSubset'
--  Streamly.toList csvTextStream >>= putStrLn . Text.unpack . Text.intercalate "\n"
  FStreamly.writeLines "exampleOut.csv" csvTextStream
--  FStreamly.writeLines "exampleOutCS.csv" csvTextStreamCS
  forestFiresMissingPath <- Paths.usePath Paths.forestFiresMissingPath
  -- try to load with ordinary row
  let tableLength :: Foldable f => f a -> Int
      tableLength = FL.fold FL.length
  forestFiresMissing :: Frames.Frame FFColSubset <- FStreamly.inCoreAoS $ FStreamly.readTableOpt fFColSubsetParser forestFiresMissingPath
  putStrLn $ "Loaded table with missing data using inferred row from complete table. Complete table has "
    <> show (tableLength forestFires)
    <> " and with missing data (1 row missing 'day' a Text entry, one missing 'wind', a Double) has "
    <> show (tableLength forestFiresMissing)
  forestFiresMissing2 :: Frames.Frame FFInferMaybe <- FStreamly.inCoreAoS $ FStreamly.readTableOpt fFInferMaybeParser forestFiresMissingPath
  putStrLn $ "Loaded the same table but with the types for 'day' and 'wind' set to OrMissing. Has "
    <> show (tableLength forestFiresMissing2)
    <> " rows."
