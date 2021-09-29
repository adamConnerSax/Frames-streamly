{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified  Data.Text as T

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

ffColSubsetRowGen :: FStreamly.RowGen 'FStreamly.ColumnByName Frames.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

ffRenameDayRowGen :: FStreamly.RowGen 'FStreamly.ColumnByName Frames.CommonColumns
ffRenameDayRowGen = FStreamly.modifyRowTypeNameAndColumnSelector "FFRenameDay" modSelector ffColSubsetRowGen where
  modSelector = FStreamly.renameSome (Map.fromList [(FStreamly.HeaderText "day", FStreamly.ColTypeName "DayOfWeek")])

ffNoHeaderRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition Frames.CommonColumns
ffNoHeaderRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFNoHeader"
    rowGen = (FStreamly.rowGen (thPath forestFiresNoHeaderPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = const $ FStreamly.noHeaderColumnsNumbered "Col"

ffIgnoreHeaderRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition  Frames.CommonColumns
ffIgnoreHeaderRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFIgnoreHeader"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = const $ FStreamly.namesGiven True $ fmap FStreamly.ColTypeName $ ("Col" <>) . T.pack . show <$> ([0..12] :: [Int])

ffIgnoreHeaderChooseNamesRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition  Frames.CommonColumns
ffIgnoreHeaderChooseNamesRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFIgnoreHeaderChooseNames"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = const
                 $ FStreamly.namedColumnNumberSubset True
                 $ fmap FStreamly.ColTypeName
                 $ Map.fromList [(0, "X"), (1,"Y"), (2, "Month"), (3, "DayOfWeek"), (8, "Temp"), (10, "Wind")]
