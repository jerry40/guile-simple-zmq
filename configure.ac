AC_INIT(guile-simple-zmq, 0.2)
AC_CONFIG_SRCDIR(simple-zmq.scm.in)
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([-Wall -Werror foreign color-tests])

dnl Enable silent rules by default.
AM_SILENT_RULES([yes])

dnl We require pkg.m4 (from pkg-config) and guile.m4 (from Guile.)
dnl Make sure they are available.
m4_pattern_forbid([PKG_CHECK_MODULES])
m4_pattern_forbid([GUILE_PKG])

GUILE_PKG([3.0 2.2 2.0])
GUILE_PROGS

PKG_CHECK_MODULES([LIBZMQ], [libzmq])
PKG_CHECK_VAR([LIBZMQ_LIBDIR], [libzmq], [libdir])
AC_MSG_CHECKING([libzmq library path])
AS_IF([test "x$LIBZMQ_LIBDIR" = "x"], [
  AC_MSG_FAILURE([Unable to identify libzmq lib path.])
])
AC_SUBST([LIBZMQ_LIBDIR])

AC_CONFIG_FILES([Makefile simple-zmq.scm])
AC_CONFIG_FILES([pre-inst-env], [chmod +x pre-inst-env])

AC_OUTPUT
