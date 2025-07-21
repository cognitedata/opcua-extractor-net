using System;
using System.IO;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Xunit;

namespace Test.Simple
{
    public class SimpleDataPointTest
    {
        [Fact]
        public void TestReceivedTimestampSerialization()
        {
            // Arrange
            var ts = DateTime.UtcNow;
            var receivedTs = DateTime.UtcNow.AddMilliseconds(100); // Different from ts to verify serialization
            var id = "test-id";
            var status = StatusCodes.Good;
            
            // Test with numeric value
            var numericDataPoint = new UADataPoint(ts, id, 123.456, status, receivedTs);
            
            // Act - Serialize
            var numericBytes = numericDataPoint.ToStorableBytes();
            
            // Act - Deserialize
            using var numericStream = new MemoryStream(numericBytes);
            var deserializedNumeric = UADataPoint.FromStream(numericStream);
            
            // Assert
            Assert.NotNull(deserializedNumeric);
            Assert.Equal(numericDataPoint.Timestamp, deserializedNumeric.Timestamp);
            Assert.Equal(numericDataPoint.ReceivedTimestamp, deserializedNumeric.ReceivedTimestamp);
            Assert.Equal(numericDataPoint.Id, deserializedNumeric.Id);
            Assert.Equal(numericDataPoint.IsString, deserializedNumeric.IsString);
            Assert.Equal(numericDataPoint.DoubleValue, deserializedNumeric.DoubleValue);
            Assert.Equal(numericDataPoint.Status.Code, deserializedNumeric.Status.Code);
            
            // Test with string value
            var stringDataPoint = new UADataPoint(ts, id, "test-string", status, receivedTs);
            
            // Act - Serialize
            var stringBytes = stringDataPoint.ToStorableBytes();
            
            // Act - Deserialize
            using var stringStream = new MemoryStream(stringBytes);
            var deserializedString = UADataPoint.FromStream(stringStream);
            
            // Assert
            Assert.NotNull(deserializedString);
            Assert.Equal(stringDataPoint.Timestamp, deserializedString.Timestamp);
            Assert.Equal(stringDataPoint.ReceivedTimestamp, deserializedString.ReceivedTimestamp);
            Assert.Equal(stringDataPoint.Id, deserializedString.Id);
            Assert.Equal(stringDataPoint.IsString, deserializedString.IsString);
            Assert.Equal(stringDataPoint.StringValue, deserializedString.StringValue);
            Assert.Equal(stringDataPoint.Status.Code, deserializedString.Status.Code);
            
            // Test with null value
            var nullDataPoint = new UADataPoint(ts, id, false, status, receivedTs); // numeric with null value
            
            // Act - Serialize
            var nullBytes = nullDataPoint.ToStorableBytes();
            
            // Act - Deserialize
            using var nullStream = new MemoryStream(nullBytes);
            var deserializedNull = UADataPoint.FromStream(nullStream);
            
            // Assert
            Assert.NotNull(deserializedNull);
            Assert.Equal(nullDataPoint.Timestamp, deserializedNull.Timestamp);
            Assert.Equal(nullDataPoint.ReceivedTimestamp, deserializedNull.ReceivedTimestamp);
            Assert.Equal(nullDataPoint.Id, deserializedNull.Id);
            Assert.Equal(nullDataPoint.IsString, deserializedNull.IsString);
            Assert.Equal(nullDataPoint.DoubleValue, deserializedNull.DoubleValue);
            Assert.Equal(nullDataPoint.Status.Code, deserializedNull.Status.Code);
        }
    }
} 