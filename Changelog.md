v0.3.1
* Added code/cabal bounds (behind “streamly9” flag) for >= streamly-0.9
* Bumped various upper bounds in cabal file

v0.3.0.0
* Changed API so tokenizing is handled when the file is read.  This changes various library function signatures.

v0.2.0.0
* Added support for using any stream type implementing the ```Streaming``` class. Backends are provided for Pipes and Streamly
* Added ability to infer and load only a subset of columns, identified either by column header or integer column position.
* Added the ability to selectively infer to a Maybe-like type (```OrMissing```) for any column.
* Added the ability to rename columns during inference, before the inferred columns are declared.
* Added the ability to, for inference, use parsers given via a Rec of parsing functions rather than instances of Parseable.

v0.1.1.1
* Removed Strictness/Memory testing cruft for release.
v0.1.2.0
* Made compatible with streamly-0.8.0

v0.1.1.0
* Added ```StrictReadRec``` class

v0.1.0.3
* Bumped some upper bounds and raised the lower bound on relude

v0.1.0.2
* Relaxed some ```MonadIO``` constraints to ```Monad``` in Frames.Streamly.CSV

v0.1.0.0
* Initial Release
