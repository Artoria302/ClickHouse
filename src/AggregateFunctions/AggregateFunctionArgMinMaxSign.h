#pragma once

#include <base/StringRef.h>
#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionMinMaxAny.h> // SingleValueDataString used in embedded compiler


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CORRUPTED_DATA;
}


/// For possible values for template parameters, see 'AggregateFunctionMinMaxAny.h'.
template <typename ResultData, typename ValueData, typename SignData>
struct AggregateFunctionArgMinMaxSignData
{
    using ResultData_t = ResultData;
    using ValueData_t = ValueData;
    using SignData_t = SignData;

    ResultData result;  // the argument at which the minimum/maximum value is reached.
    ValueData value;    // value for which the minimum/maximum is calculated.
    SignData sign = 0;  // sign for collapse.

    static bool allocatesMemoryInArena()
    {
        return ResultData::allocatesMemoryInArena() || ValueData::allocatesMemoryInArena();
    }
};

/// Returns the first arg value found for the minimum/maximum value. Example: argMaxSign(arg, value, sign).
template <typename Data>
class AggregateFunctionArgMinMaxSign final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMaxSign<Data>>
{
private:
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMaxSign<Data>>;
    using ColVecType = ColumnVectorOrDecimal<typename Data::SignData_t>;

public:
    AggregateFunctionArgMinMaxSign(const DataTypePtr & type_res_, const DataTypePtr & type_val_, const DataTypePtr & type_sign_)
        : Base({type_res_, type_val_, type_sign_}, {}, makeNullable(type_res_))
        , type_val(this->argument_types[1])
        , serialization_res(type_res_->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
    {
        if (!type_val->isComparable())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of "
                            "aggregate function {} because the values of that data type are not comparable",
                            type_val->getName(), getName());
    }

    String getName() const override
    {
        return StringRef(Data::ValueData_t::name()) == StringRef("min") ? "argMinSign" : "argMaxSign";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).sign += assert_cast<const ColVecType &>(*columns[2]).getData()[row_num];
        if (this->data(place).sign == 0)
        {
            this->data(place).value.unset();
            this->data(place).result.unset();
        }
        else if (this->data(place).value.changeIfBetter(*columns[1], row_num, arena))
                this->data(place).result.change(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).sign += this->data(rhs).sign;
        if (this->data(place).sign == 0)
        {
            this->data(place).value.unset();
            this->data(place).result.unset();
        }
        else if (this->data(place).value.changeIfBetter(this->data(rhs).value, arena))
            this->data(place).result.change(this->data(rhs).result, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).result.write(buf, *serialization_res);
        this->data(place).value.write(buf, *serialization_val);
        writeBinary(this->data(place).sign, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).result.read(buf, *serialization_res, arena);
        this->data(place).value.read(buf, *serialization_val, arena);
        readBinary(this->data(place).sign, buf);
        if (unlikely(this->data(place).value.has() != this->data(place).result.has()))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid state of the aggregate function {}: has_value ({}) != has_result ({})",
                getName(),
                this->data(place).value.has(),
                this->data(place).result.has());
    }

    bool allocatesMemoryInArena() const override
    {
        return Data::allocatesMemoryInArena();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if (this->data(place).sign == 0)
            to.insertDefault();
        else
        {
            auto & column = static_cast<ColumnNullable &>(to);
            this->data(place).result.insertResultInto(column.getNestedColumn());
            column.getNullMapData().push_back(0);
        }
    }
};

}
