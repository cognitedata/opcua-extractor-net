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
