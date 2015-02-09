PROJECT = erater
SHELL_OPTS = +P 2000000 +sbwt none +sbt ts

DEPS = gproc
dep_gproc = git https://github.com/uwiger/gproc.git e5500cd5fb950813e60827d337767c0b4237aa74

include erlang.mk
