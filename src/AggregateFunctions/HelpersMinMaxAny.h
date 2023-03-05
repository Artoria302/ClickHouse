#pragma once

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/AggregateFunctionArgMinMaxSign.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
struct Settings;

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>>(argument_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DateTime64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal32>>>(argument_type);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal128>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<Data<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}


/// argMin, argMax
template <template <typename> class MinMaxData, typename ResData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type)
{
    WhichDataType which(val_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<TYPE>>>>(res_type, val_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>>(res_type, val_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>(res_type, val_type);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DateTime64>>>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal32>>>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal64>>>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal128>>>>(res_type, val_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataString>>>(res_type, val_type);

    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataGeneric>>>(res_type, val_type);
}

template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    WhichDataType which(res_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<TYPE>>(res_type, val_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(res_type, val_type);
    if (which.idx == TypeIndex::DateTime)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(res_type, val_type);
    if (which.idx == TypeIndex::DateTime64)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DateTime64>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal32)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal32>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal64)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal64>>(res_type, val_type);
    if (which.idx == TypeIndex::Decimal128)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal128>>(res_type, val_type);
    if (which.idx == TypeIndex::String)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataString>(res_type, val_type);

    return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataGeneric>(res_type, val_type);
}


/// argMinSign, argMaxSign
#define FOR_SIGN_TYPES(M) \
    M(Int8) \
    M(Int16) \
    M(Int32)

template <typename ResData, typename MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSignThird(const DataTypePtr & res_type, const DataTypePtr & val_type, const DataTypePtr & sign_type)
{
    WhichDataType which(sign_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionArgMinMaxSign<AggregateFunctionArgMinMaxSignData<ResData, MinMaxData, TYPE>>(res_type, val_type, sign_type); /// NOLINT
    FOR_SIGN_TYPES(DISPATCH)
#undef DISPATCH

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal sign type, argMinSign and argMaxSign sign type only support Int8, Int16, Int32");
}


template <template <typename> class MinMaxData, typename ResData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSignSecond(const DataTypePtr & res_type, const DataTypePtr & val_type, const DataTypePtr & sign_type)
{
    WhichDataType which(val_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<TYPE>>>(res_type, val_type, sign_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::DateTime)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::DateTime64)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<DateTime64>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal32)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<Decimal32>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal64)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<Decimal64>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal128)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataFixed<Decimal128>>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::String)
        return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataString>>(res_type, val_type, sign_type);

    return createAggregateFunctionArgMinMaxSignThird<ResData, MinMaxData<SingleValueDataGeneric>>(res_type, val_type, sign_type);
}

template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSign(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    if (argument_types.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires three argument",
                        name);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];
    const DataTypePtr & sign_type = argument_types[2];

    if (res_type->isNullable() || val_type->isNullable() || sign_type->isNullable())
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} requires three none nullable argument",
                        name);
    }

    WhichDataType which(res_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<TYPE>>(res_type, val_type, sign_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::DateTime)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::DateTime64)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<DateTime64>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal32)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<Decimal32>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal64)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<Decimal64>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::Decimal128)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataFixed<Decimal128>>(res_type, val_type, sign_type);
    if (which.idx == TypeIndex::String)
        return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataString>(res_type, val_type, sign_type);

    return createAggregateFunctionArgMinMaxSignSecond<MinMaxData, SingleValueDataGeneric>(res_type, val_type, sign_type);
}

}
