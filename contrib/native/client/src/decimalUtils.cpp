#include "decimalUtils.hpp"

#include "recordBatch.h"
#include <vector>

#ifdef _WIN32
#define bswap_16                    _byteswap_ushort
#define bswap_32                    _byteswap_ulong
#define bswap_64                    _byteswap_uint64
#elif defined(__linux__)
#include <byteswap.h>   //for bswap_16,32,64
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define bswap_16 OSSwapInt16
#define bswap_32 OSSwapInt32
#define bswap_64 OSSwapInt64
#endif

using namespace boost::multiprecision;

#define MAX_DIGITS 9
#define INTEGER_SIZE sizeof(uint32_t)
#define DIGITS_BASE 1000000000

namespace Drill
{

    // Code is ported from Java DecimalUtility.java implementation.
    // The big differences are that we expose a DecimalValue struct,
    // We store the unscaled value in a boost cpp_int
    // And that when we retrieve integer data off the wire, we need to do an endianness swap on
    // on little-endian platforms.

DecimalValue getDecimalValueFromByteBuf(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale, bool truncateScale)
{

    // For sparse decimal type we have padded zeroes at the end, strip them while converting to BigDecimal.
    int32_t actualDigits;

    // Initialize the BigDecimal, first digit in the ByteBuf has the sign so mask it out
    cpp_int decimalDigits = bswap_32(data.getUint32(startIndex) & 0x7FFFFFFF);
    
    cpp_int base(DIGITS_BASE);
    
    for (int i = 1; i < nDecimalDigits; i++) {
    
        // Note: we need to byteswap when we retrieve integers off the wire since they are
        // stored in big-endian format.
        cpp_int temp = bswap_32(data.getUint32(startIndex + (i * INTEGER_SIZE)));
        decimalDigits *= base;
        decimalDigits += temp;
    }
    
    // Truncate any additional padding we might have added
    if (truncateScale && scale > 0 && (actualDigits = scale % MAX_DIGITS) != 0) {
        cpp_int truncate = (int32_t) std::pow(10.0, (MAX_DIGITS - actualDigits));
        decimalDigits /= truncate;
    }
    
    DecimalValue val;
    val.m_unscaledValue = decimalDigits;

    // set the sign
    if (data.getUint32((startIndex) & 0x80000000) != 0)
    {
        val.m_unscaledValue *= -1;
    }

    val.m_scale = scale;
    return val;
}

DecimalValue getDecimalValueFromDense(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale, int maxPrecision, int width)
{
       /* This method converts the dense representation to
          * an intermediate representation. The intermediate
          * representation has one more integer than the dense
          * representation.
          */
    std::vector<uint8_t> intermediateBytes((nDecimalDigits + 1) * INTEGER_SIZE, 0);

    int32_t intermediateIndex = 3;

    int32_t mask[] = {0x03, 0x0F, 0x3F, 0xFF};
    int32_t reverseMask[] = {0xFC, 0xF0, 0xC0, 0x00};

    int32_t maskIndex;
    int32_t shiftOrder;
    uint8_t shiftBits;

    // TODO: Some of the logic here is common with casting from Dense to Sparse types, factor out common code
    if (maxPrecision == 38) {
        maskIndex = 0;
        shiftOrder = 6;
        shiftBits = 0x00;
        intermediateBytes[intermediateIndex++] = (uint8_t) (data.getByte(startIndex) & 0x7F);
    } else if (maxPrecision == 28) {
        maskIndex = 1;
        shiftOrder = 4;
        shiftBits = (uint8_t) ((data.getByte(startIndex) & 0x03) << shiftOrder);
        intermediateBytes[intermediateIndex++] = (uint8_t) (((data.getByte(startIndex) & 0x3C) & 0xFF) >> 2);
    } else {
        assert("Dense types with max precision 38 and 28 are only supported");
    }

    int32_t inputIndex = 1;
    bool sign = false;

    if ((data.getByte(startIndex) & 0x80) != 0) {
        sign = true;
    }
    
    while (inputIndex < width) {
    
        intermediateBytes[intermediateIndex] = (uint8_t) ((shiftBits) | (((data.getByte(startIndex + inputIndex) & reverseMask[maskIndex]) & 0xFF) >> (8 - shiftOrder)));
        
        shiftBits = (uint8_t) ((data.getByte(startIndex + inputIndex) & mask[maskIndex]) << shiftOrder);
        
        inputIndex++;
        intermediateIndex++;
        
        if (((inputIndex - 1) % INTEGER_SIZE) == 0) {
            shiftBits = (uint8_t) ((shiftBits & 0xFF) >> 2);
            maskIndex++;
            shiftOrder -= 2;
        }

    }
    /* copy the last byte */
    intermediateBytes[intermediateIndex] = shiftBits;
    
    if (sign) {
        intermediateBytes[0] = (uint8_t) (intermediateBytes[0] | 0x80);
    }
    
    SlicedByteBuf intermediateData(&intermediateBytes[0], 0, intermediateBytes.size());
    return getDecimalValueFromIntermediate(intermediateData, 0, nDecimalDigits + 1, scale);
}

}

