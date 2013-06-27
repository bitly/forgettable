# GoForget

## Building

Simply build with `go build` and install with `go install`!

Requirements:

* [redigo](http://github.com/garyburd/redigo) (must be after commit 69e1a27a where redis.Pool.TestOnBorrow was introduced)
* [redis](http://redis.io/) v2.7 or higher


## Running

To start the service run `goforget -redis-host=localhost:6379:1 -http=:8080`.
This will start an instance of `goforget` on port 8080 and will be connected to
a local redis server on port 6379 using database 1.  To see other valid
options, type `goforget --help`!

## Endpoints

* Increment
  * `/incr?distribution=colors&field=red`
  * This will create the distribution if necessary and increase the probability
    of the field red by increasing it's count by one.  There is an optional
    parameter, `N`, to increase the count by an arbitrary amount.

* Access entire distribution
  * `/dist?distribution=colors`

* Access single field
  * `/get?distribution=colors&field=blue`

* Access N fields with highest probability
  * `/nmostprobable?distribution=colors&N=10`

* Get the number of distributions currently being stores
  * `/dbsize`
