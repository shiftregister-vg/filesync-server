# Copyright 2008-2015 Canonical
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# For further info, check  http://launchpad.net/filesync-server

"""SSL utils for the api server."""

import ctypes
import glob


# disable ssl compression
def disable_ssl_compression(logger):
    """Disable ssl compression.

    This is done using ctypes as OpenSSL-0.9.8 doesn't provide an API for
    doing it. This is available in versions >= 0.9.9.

    This code is from:
        http://journal.paul.querna.org/articles/2011/04/05/openssl-memory-use/
    """
    logger.info('Disabling ssl compression.')
    try:
        # try to use the default location to load libssl
        openssl = ctypes.CDLL(None, ctypes.RTLD_GLOBAL)
        try:
            openssl.SSL_COMP_get_compression_methods
        except AttributeError:
            # fallback to the first version we found of libssl.so.<version>
            ssllib = sorted(glob.glob("/usr/lib/libssl.so.*"))[0]
            openssl = ctypes.CDLL(ssllib, ctypes.RTLD_GLOBAL)

        openssl.SSL_COMP_get_compression_methods.restype = ctypes.c_void_p
        openssl.sk_zero.argtypes = [ctypes.c_void_p]
        openssl.sk_zero(openssl.SSL_COMP_get_compression_methods())
    except Exception:
        logger.exception('Disable SSL Compression: Failed')
