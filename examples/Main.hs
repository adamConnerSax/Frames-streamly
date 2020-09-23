{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import qualified DemoPaths as Paths
import qualified Frames
--import qualified Frames.TH as Frames
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.Transform as FStreamly
import qualified Data.Vinyl as V
import qualified Text.Printf as Printf
import qualified Streamly.Prelude as Streamly
import Data.Text (Text) -- for Frames template splicing 
import qualified Data.Text as Text

Frames.tableTypes "ForestFires" (Paths.thPath Paths.forestFiresPath)

main :: IO ()
main = do
  -- load the file via
  forestFiresPath <- Paths.usePath Paths.forestFiresPath 
  forestFires :: Frames.Frame ForestFires <- FStreamly.inCoreAoS $ FStreamly.readTable forestFiresPath
  let filterAndMap :: ForestFires -> Maybe (Frames.Record [X, Y, Month, Temp, Wind])
      filterAndMap r = if Frames.rgetField @Day r == "fri" then (Just $ Frames.rcast r) else Nothing
      forestFires' = FStreamly.mapMaybe filterAndMap forestFires
      formatRow = FStreamly.formatWithShow
              V.:& FStreamly.formatWithShow
              V.:& FStreamly.formatTextAsIs
              V.:& FStreamly.liftFieldFormatter (Text.pack . Printf.printf "%.1f")
              V.:& FStreamly.liftFieldFormatter (Text.pack . Printf.printf "%.1f")
              V.:& V.RNil
      csvTextStream = FStreamly.streamSV' formatRow "," $ Streamly.fromFoldable forestFires'
  Streamly.toList csvTextStream >>= putStrLn . Text.unpack . Text.intercalate "\n"
  
  

