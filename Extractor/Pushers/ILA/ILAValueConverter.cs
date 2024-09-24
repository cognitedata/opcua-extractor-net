using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using Cognite.Extractor.Common;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.ILA
{
    public class ILAValueConverter
    {
        private readonly StringConverter converter;

        public StringConverter Converter => converter;
        public ILAValueConverter(StringConverter converter)
        {
            this.converter = converter;
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

        private string ConvertDateTime(DateTime dt)
        {
            return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffK", CultureInfo.InvariantCulture);
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
                    return new RawPropertyValue<string>(ConvertDateTime(Convert.ToDateTime(value.Value)));
                case PropertyTypeVariant.text:
                    return new RawPropertyValue<string>(converter.ConvertToString(value.Value, null, null, StringConverterMode.Simple));
                case PropertyTypeVariant.json:
                    var val = converter.ConvertToString(value, null, null, StringConverterMode.ReversibleJson);
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

            return variant switch
            {
                PropertyTypeVariant.int32 => new RawPropertyValue<int[]>(enm.Cast<object>().Select(v => Convert.ToInt32(v)).ToArray()),
                PropertyTypeVariant.int64 => new RawPropertyValue<long[]>(enm.Cast<object>().Select(v => Convert.ToInt64(v)).ToArray()),
                PropertyTypeVariant.float32 => new RawPropertyValue<float[]>(enm.Cast<object>().Select(v => Convert.ToSingle(v)).ToArray()),
                PropertyTypeVariant.float64 => new RawPropertyValue<double[]>(enm.Cast<object>().Select(v => Convert.ToDouble(v)).ToArray()),
                PropertyTypeVariant.timestamp or PropertyTypeVariant.date => new RawPropertyValue<string[]>(enm.Cast<object>()
                    .Select(v => ConvertDateTime(Convert.ToDateTime(v))).ToArray()),
                PropertyTypeVariant.text => new RawPropertyValue<string[]>(enm.Cast<object>()
                                        .Select(v => converter.ConvertToString(value.Value, null, null, StringConverterMode.Simple))
                                        .ToArray()),
                PropertyTypeVariant.boolean => new RawPropertyValue<bool>(Convert.ToBoolean(value.Value)),
                _ => null,
            };
        }
    }
}
