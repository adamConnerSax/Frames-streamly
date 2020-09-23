module DemoPaths where

import qualified Frames.TH as Frames
import qualified Paths_Frames_streamly as Paths

forestFiresPath :: FilePath
forestFiresPath = "forestfires.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x


usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir
