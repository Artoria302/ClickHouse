#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionArgMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionArgMaxSign(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMaxSign<AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);

    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("argMax", { createAggregateFunctionArgMax, properties });

    AggregateFunctionProperties sign_properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("argMaxSign", { createAggregateFunctionArgMaxSign, sign_properties });
}

}
