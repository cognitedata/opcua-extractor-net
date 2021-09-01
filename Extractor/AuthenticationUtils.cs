/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Opc.Ua;
using System.Security.Cryptography.X509Certificates;

namespace Cognite.OpcUa
{
    static class AuthenticationUtils
    {
        private static X509Certificate2? GetCertificate(X509CertConfig certConf)
        {
            if (certConf == null) return null;
            if (certConf.Store != X509CertificateLocation.None)
            {
                var store = new X509Store(
                    certConf.Store == X509CertificateLocation.Local
                    ? StoreLocation.LocalMachine
                    : StoreLocation.CurrentUser);
                try
                {
                    store.Open(OpenFlags.ReadOnly);

                    var certCollection = store.Certificates;

                    var certificates = certCollection
                        .Find(X509FindType.FindBySubjectDistinguishedName, certConf.CertName, true);
                    if (certificates.Count == 0) return null;

                    return certificates[0];
                }
                finally
                {
                    store.Close();
                }
            }

            if (string.IsNullOrEmpty(certConf.FileName)) return null;

            X509Certificate2 cert;
            if (!string.IsNullOrEmpty(certConf.Password))
            {
                cert = new X509Certificate2(certConf.FileName, certConf.Password);
            }
            else
            {
                cert = new X509Certificate2(certConf.FileName);
            }
            return cert;
        }

        public static UserIdentity GetUserIdentity(UAClientConfig config)
        {
            if (!string.IsNullOrEmpty(config.Username)) return new UserIdentity(config.Username, config.Password);
            if (config.X509Certificate != null)
            {
#pragma warning disable CA2000 // Dispose objects before losing scope. Owned by UAClient
                var cert = GetCertificate(config.X509Certificate);
#pragma warning restore CA2000 // Dispose objects before losing scope

                return new UserIdentity(cert);
            }

            return new UserIdentity(new AnonymousIdentityToken());
        }
    }
}
