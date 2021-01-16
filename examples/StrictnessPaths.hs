{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
module StrictnessPaths where

import qualified Frames as F
import qualified Frames.TH                     as F
import qualified Data.Text as T

pumsACS1YrCSV :: FilePath
pumsACS1YrCSV = "example_data/acs100k.csv"


pumsACS1YrRowGen = (F.rowGen pumsACS1YrCSV) { F.tablePrefix = "PUMS"
                                            , F.separator   = ","
                                            , F.rowTypeName = "PUMS_Raw"
                                            }

type PUMSSTATEFIP = "STATEFIP" F.:-> Int
