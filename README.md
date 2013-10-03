# Teradata::Cli

Teradata::Cli is an access module which allows Ruby scripts
to access Teradata RDBMS by CLIv2 interface.

It's based on the [Ruby/CLIv2](http://sourceforge.net/projects/rubycli/) library.


## Requirements

    CLIv2 (32bit / 64bit)
    C compiler


## Installation

Add this line to your application's Gemfile:

    gem 'teradata-cli'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install teradata-cli


## Usage

Look the examples folder


## Tests

    $ export TEST_LOGON_STRING=dbc/user,password
    $ ruby -Ilib:test test/all


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
