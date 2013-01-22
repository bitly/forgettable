Forget-Table
============

Forget-Table is a database for storing non-stationary categorical distributions
that forget old observations responsibly.  It has been designed to store
millions of distributions and can be written to at a high volume.

This repo includes two implemintations of the forget-table concept, both using
[redis](http://redis.io) as a backend.  The implemintations are:

* pyforget - a quick and dirty implemintation meant to be used as a playground
* goforget - written in GO for great speed and scalibility.  This has a much
  stricter API and is much more stable to use

The individual implemintations have their own docs on usage.

Created by [Micha Gorelick](http://micha.gd/), [Mike
Dewar](http://twitter.com/mikedewar) with the help of [Dan
Frank](http://www.danielhfrank.com/) and all the amazing engineers and
scientists at [bitly](https://bitly.com/pages/about).

