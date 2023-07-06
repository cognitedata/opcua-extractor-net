using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Opc.Ua;
using System;
using System.Collections;
using System.Linq;
using System.Text.Json;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class DMSValueConverter
    {
        private readonly StringConverter converter;

        public StringConverter Converter => converter;
        private readonly string space;
        public DMSValueConverter(StringConverter converter, string space)
        {
            this.converter = converter;
            this.space = space;
        }

        public IDMSValue? ConvertVariant(BasePropertyType? type, Variant? value)
        {
            if (value?.Value is null) return null;

            var variant = type?.Type ?? PropertyTypeVariant.json;
            var isArray = type?.GetType()?.GetProperty("List")?.GetValue(type) as bool? ?? false;
            
            if (isArray)
            {
                return ConvertArrayVariant(variant, value.Value);
            }
            else
            {
                return ConvertScalarVariant(variant, value.Value);
            }
        }

        private IDMSValue? ConvertScalarVariant(PropertyTypeVariant variant, Variant value)
        {
            switch (variant)
            {
                case PropertyTypeVariant.int32:
                    return new RawPropertyValue<int>(Convert.ToInt32(value.Value));
                case PropertyTypeVariant.int64:
                    return new RawPropertyValue<long>(Convert.ToInt64(value.Value));
                case PropertyTypeVariant.float32:
                    return new RawPropertyValue<float>(Convert.ToSingle(value.Value));
                case PropertyTypeVariant.float64:
                    return new RawPropertyValue<double>(Convert.ToDouble(value.Value));
                case PropertyTypeVariant.timestamp:
                case PropertyTypeVariant.date:
                    return new RawPropertyValue<DateTime>(Convert.ToDateTime(value.Value));
                case PropertyTypeVariant.text:
                    return new RawPropertyValue<string>(converter.ConvertToString(value.Value, null, null, StringConverterMode.Simple));
                case PropertyTypeVariant.direct:
                    if (value.Value is NodeId id && !id.IsNullNodeId)
                    {
                        return new RawPropertyValue<DirectRelationIdentifier>(new DirectRelationIdentifier(space, id.ToString()));
                    }
                    return null;
                case PropertyTypeVariant.json:
                    var val = converter.ConvertToString(value, null, null, StringConverterMode.ReversibleJson);
                    Console.WriteLine(val + ", " + value.TypeInfo);
                    var json = JsonDocument.Parse(val);
                    return new RawPropertyValue<JsonElement>(json.RootElement);
                case PropertyTypeVariant.boolean:
                    return new RawPropertyValue<bool>(Convert.ToBoolean(value.Value));
            }
            return null;
        }

        private IDMSValue? ConvertArrayVariant(PropertyTypeVariant variant, Variant value)
        {
            if (value.Value is not IEnumerable enm) return ConvertArrayVariant(variant, new Variant(new[] { value.Value }));

            switch (variant)
            {
                case PropertyTypeVariant.int32:
                    return new RawPropertyValue<int[]>(enm.Cast<object>().Select(v => Convert.ToInt32(v)).ToArray());
                case PropertyTypeVariant.int64:
                    return new RawPropertyValue<long[]>(enm.Cast<object>().Select(v => Convert.ToInt64(v)).ToArray());
                case PropertyTypeVariant.float32:
                    return new RawPropertyValue<float[]>(enm.Cast<object>().Select(v => Convert.ToSingle(v)).ToArray());
                case PropertyTypeVariant.float64:
                    return new RawPropertyValue<double[]>(enm.Cast<object>().Select(v => Convert.ToDouble(v)).ToArray());
                case PropertyTypeVariant.timestamp:
                case PropertyTypeVariant.date:
                    return new RawPropertyValue<DateTime[]>(enm.Cast<object>().Select(v => Convert.ToDateTime(v)).ToArray());
                case PropertyTypeVariant.text:
                    return new RawPropertyValue<string[]>(enm.Cast<object>()
                        .Select(v => converter.ConvertToString(value.Value, null, null, StringConverterMode.Simple))
                        .ToArray());
                case PropertyTypeVariant.boolean:
                    return new RawPropertyValue<bool>(Convert.ToBoolean(value.Value));
            }

            return null;
        }
    }
}
