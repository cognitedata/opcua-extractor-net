using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class DMSValueConverter
    {
        private readonly StringConverter converter;
        public DMSValueConverter(StringConverter converter)
        {
            this.converter = converter;
        }

        public IDMSValue? ConvertVariant(PropertyTypeVariant type, Variant? value)
        {
            if (value?.Value is null) return null;

            switch (type)
            {
                case PropertyTypeVariant.int32:
                    return new RawPropertyValue<int>(Convert.ToInt32(value.Value.Value));
                case PropertyTypeVariant.int64:
                    return new RawPropertyValue<long>(Convert.ToInt64(value.Value.Value));
                case PropertyTypeVariant.float32:
                    return new RawPropertyValue<float>(Convert.ToSingle(value.Value.Value));
                case PropertyTypeVariant.float64:
                    return new RawPropertyValue<double>(Convert.ToDouble(value.Value.Value));
                // TODO, figure out formatting for these...
                case PropertyTypeVariant.timestamp:
                case PropertyTypeVariant.date:
                    return new RawPropertyValue<DateTime>(Convert.ToDateTime(value.Value.Value));
                case PropertyTypeVariant.text:
                case PropertyTypeVariant.direct:
                    return new RawPropertyValue<string>(value.Value.Value.ToString());
                case PropertyTypeVariant.json:
                    var val = converter.ConvertToString(value, null, null, Types.StringConverterMode.ReversibleJson);
                    Console.WriteLine(val + ", " + value.Value.TypeInfo);
                    var json = JsonDocument.Parse(val);
                    return new RawPropertyValue<JsonElement>(json.RootElement);
                case PropertyTypeVariant.boolean:
                    return new RawPropertyValue<bool>(Convert.ToBoolean(value.Value.Value));
            }
            return null;
        }
    }
}
