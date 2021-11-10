using Xunit;

namespace Test.Utils
{
    [CollectionDefinition("Shared server tests")]
    public class SharedServerTests : ICollectionFixture<StaticServerTestFixture>
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
