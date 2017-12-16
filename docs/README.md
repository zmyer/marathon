# Marathon Docs and Website

## Run it locally

Ensure you have installed everything listed in the dependencies section before
following the instructions.

### Dependencies

* [Bundler](http://bundler.io/)
* [Node.js](http://nodejs.org/) (for compiling assets)
* Python
* Ruby
* [RubyGems](https://rubygems.org/)

### Instructions

#### Using Docker

1. Build the docker image:

        docker build . -t jekyll

2. Run it (from this folder)

        docker run --rm -it -v $(pwd):/site-docs -p 4000:4000 jekyll

3. Visit the site at
   [http://localhost:4000/marathon/](http://localhost:4000/marathon/)

#### Native OS

1. Install packages needed to generate the site

    * On Linux:

            $ apt-get install ruby-dev make autoconf nodejs nodejs-legacy python-dev npm

    * On Mac OS X:

            $ brew install node

2. Clone the Marathon repository

3. Change into the "docs" directory where docs live

        $ cd docs/

4. Install Bundler

        $ gem install bundler

5. Install the bundle's dependencies

        $ bundle install --path vendor/bundle

6. Start the web server

        $ bundle exec jekyll serve --watch

7. Visit the site at
   [http://localhost:4000/marathon/](http://localhost:4000/marathon/)

## Deploying the site

1. Clone a separate copy of the Marathon repo as a sibling of your normal
   Marathon project directory and name it "marathon-gh-pages".

        $ git clone git@github.com:mesosphere/marathon.git marathon-gh-pages

2. Check out the "gh-pages" branch.

        $ cd /path/to/marathon-gh-pages
        $ git checkout gh-pages

3. Check out the appropriate release branch, then copy the contents of the "docs" directory in master to the root of your
   marathon-gh-pages directory.

        $ cd /path/to/marathon
        $ git checkout releases/1.x
        $ # to make sure we also remove deleted documentation, we need to delete all files first.
        $ # please note, rm -r ../marathon-gh-pages/* will not delete dot-files
        $ rm -r ../marathon-gh-pages/*
        $ cp -r docs/** ../marathon-gh-pages

4. Change to the marathon-gh-pages directory, commit, and push the changes

        $ cd /path/to/marathon-gh-pages
        $ git commit . -m "Syncing docs with release branch"
        $ git push
