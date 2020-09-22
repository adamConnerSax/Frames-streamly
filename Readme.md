# Frames-streamly- v 0.1.0.0

[![Build Status][travis-badge]][travis]
[![Hackage][hackage-badge]][hackage]
[![Hackage Dependencies][hackage-deps-badge]][hackage-deps]

This library contains some useful functions for using the 
[Frames](https://hackage.haskell.org/package/Frames) 
package with [streamly](https://hackage.haskell.org/package/streamly).

Frames has some built-in dependencies on the 
[Pipes](https://hackage.haskell.org/package/pipes) package,
a few of which--primarily file I/O-- require users of Frames to use
the Pipes package explicitly.  Streamly provides much of the same 
functionality as Pipes and may be some users preferred interface.

This package replicates all the external-facing bits of Frames that
rely on Pipes and uses Streamly instead.  It also fleshes out the Frames
API in a couple of places:

1. It adds some flexibility to the functions to write CSV files.  
Frames supported formatting of fields for CSV 
via a typeclass ```ShowCSV```.  That is supported here as well.  But
this package also supports using the ```Show``` instance and
creating field-by-field formatting on the fly for specific output via
a [Vinyl](https://hackage.haskell.org/package/vinyl) record of functions.
Helpful combinators are provided for formatting any single field with
a ```Show``` instance or a ```ShowCSV``` instance or a user provided
function from the field type to ```Text```. 

2. It adds some (experimental) support for ```Frame``` transformations 
using Streamly streams as in intermediate state.  
This allows use of the concurrent features of 
Streamly for functions like ```mapM``` or ```mapMaybeM```.  

3. It adds Streamly folds for the various stream to in-core 
transformations in case users want to use them directly 
in stream to Frame transformations.
_______

LICENSE (BSD-3-Clause)
_______
Copyright (c) 2020, Adam Conner-Sax, All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials provided
      with the distribution.

    * Neither the name of Adam Conner-Sax nor the names of other
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


[travis]:        <https://travis-ci.org/adamConnerSax/Frames-streamly>
[travis-badge]:  <https://travis-ci.org/adamConnerSax/Frames-streamly.svg?branch=master>
[hackage]:       <https://hackage.haskell.org/package/Frames-streamly>
[hackage-badge]: <https://img.shields.io/hackage/v/Frames-streamly.svg>
[hackage-deps-badge]: <https://img.shields.io/hackage-deps/v/Frames-streamly.svg>
[hackage-deps]: <http://packdeps.haskellers.com/feed?needle=Frames-streamly>
