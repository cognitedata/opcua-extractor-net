using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Utils
{
    [CollectionDefinition("Shared server tests")]
    public class SharedServerTests: ICollectionFixture<StaticServerTestFixture>
    {
    }

    public class StaticServerTestFixture : BaseExtractorTestFixture
    {
        public StaticServerTestFixture() : base()
        {
            DeleteFiles("fb-");
        }
    }
}
