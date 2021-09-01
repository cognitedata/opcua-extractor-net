namespace System.Diagnostics.CodeAnalysis
{
    using global::System;

    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property, Inherited = false, AllowMultiple = true)]
    [ExcludeFromCodeCoverage, DebuggerNonUserCode]
    internal sealed class MemberNotNullWhenAttribute : Attribute
    {
        public bool ReturnValue { get; }
        public string[] Members { get; }
#pragma warning disable CA1019 // Define accessors for attribute arguments
        public MemberNotNullWhenAttribute(bool returnValue, string member)
#pragma warning restore CA1019 // Define accessors for attribute arguments
        {
            ReturnValue = returnValue;
            Members = new[] { member };
        }
        public MemberNotNullWhenAttribute(bool returnValue, params string[] members)
        {
            ReturnValue = returnValue;
            Members = members;
        }
    }
}
