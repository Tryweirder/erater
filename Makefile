PROJECT = erater
SHELL_OPTS = +P 2000000 +sbwt none +sbt ts

DEPS = gproc minishard
dep_gproc = git https://github.com/uwiger/gproc.git e5500cd5fb950813e60827d337767c0b4237aa74
dep_minishard = git https://github.com/stolen/minishard.git d71f4316973039976a066414c91679210b5308f6

NID := 1
SHELL_OPTS = -sname erater$(NID) -setcookie erater_demo -s erater -boot start_sasl -sasl errlog_type error -config sample.config

include erlang.mk
