#!/usr/bin/python

from photobucket import Photobucket


pbucket = Photobucket("postgres", "password", "photobucket")
pbucket.run()
