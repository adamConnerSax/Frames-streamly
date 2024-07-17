{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified  Data.Text as T
import qualified Frames.Streamly.TH as FStreamly

forestFiresPath :: FilePath
forestFiresPath = "forestFires.csv"

forestFiresNoHeaderPath :: FilePath
forestFiresNoHeaderPath = "forestFiresNoHeader.csv"

forestFiresFewerColsPath :: FilePath
forestFiresFewerColsPath = "forestFiresFewerCols.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

ffColSubsetRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns)
ffColSubsetRowGenIO = do
  fp <- usePath forestFiresPath
  let rowTypeName = "FFColSubset"
      rowGen = (FStreamly.rowGen fp) { FStreamly.rowTypeName = rowTypeName }
      modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])
  pure $ FStreamly.modifyColumnSelector modSelector rowGen

ffRenameDayRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns)
ffRenameDayRowGenIO = do
  let modSelector = FStreamly.renameSomeUsingNames (Map.fromList [(FStreamly.HeaderText "day", FStreamly.ColTypeName "DayOfWeek")])
  rg <- ffColSubsetRowGenIO
  pure $ FStreamly.modifyRowTypeNameAndColumnSelector "FFRenameDay" modSelector rg

ffNoHeaderRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByPosition Frames.CommonColumns)
ffNoHeaderRowGenIO = do
  fp <- usePath forestFiresNoHeaderPath
  let rowTypeName = "FFNoHeader"
      rowGen = (FStreamly.rowGen fp) { FStreamly.rowTypeName = rowTypeName }
      modSelector = const $ FStreamly.noHeaderColumnsNumbered "Col"
  pure $ FStreamly.modifyColumnSelector modSelector rowGen

ffIgnoreHeaderRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByPosition  Frames.CommonColumns)
ffIgnoreHeaderRowGenIO = do
  fp <- usePath forestFiresPath
  let rowTypeName = "FFIgnoreHeader"
      rowGen = (FStreamly.rowGen fp) { FStreamly.rowTypeName = rowTypeName }
      modSelector = const $ FStreamly.namesGiven True $ fmap FStreamly.ColTypeName $ ("Col" <>) . T.pack . show <$> ([0..12] :: [Int])
  pure $ FStreamly.modifyColumnSelector modSelector rowGen

ffIgnoreHeaderChooseNamesRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByPosition  Frames.CommonColumns)
ffIgnoreHeaderChooseNamesRowGenIO = do
  fp <- usePath forestFiresPath
  let rowTypeName = "FFIgnoreHeaderChooseNames"
      rowGen = (FStreamly.rowGen fp) { FStreamly.rowTypeName = rowTypeName }
      modSelector = const
                    $ FStreamly.namedColumnNumberSubset True
                    $ fmap FStreamly.ColTypeName
                    $ Map.fromList [(0, "X"), (1,"Y"), (2, "Month"), (3, "DayOfWeek"), (8, "Temp"), (10, "Wind")]
  pure $ FStreamly.modifyColumnSelector modSelector rowGen
