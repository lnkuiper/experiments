#include <stddef.h>
#include <string.h>
#include <stdint.h>

template <size_t SIZE>
static inline void memcpy_fixed(void *dest, const void *src) {
	memcpy(dest, src, SIZE);
}

template <size_t SIZE>
static inline int memcmp_fixed(const void *str1, const void *str2) {
    return memcmp(str1, str2, SIZE);
}

namespace duckdb {

//! This templated memcpy is significantly faster than std::memcpy,
//! but only when you are calling memcpy with a const size in a loop.
//! For instance `while (<cond>) { memcpy(<dest>, <src>, const_size); ... }`
static inline void fast_memcpy(void *dest, const void *src, const size_t size) {
	switch (size) {
    case 0:
        return;
	case 1:
		return memcpy_fixed<1>(dest, src);
	case 2:
		return memcpy_fixed<2>(dest, src);
	case 3:
		return memcpy_fixed<3>(dest, src);
	case 4:
		return memcpy_fixed<4>(dest, src);
	case 5:
		return memcpy_fixed<5>(dest, src);
	case 6:
		return memcpy_fixed<6>(dest, src);
	case 7:
		return memcpy_fixed<7>(dest, src);
	case 8:
		return memcpy_fixed<8>(dest, src);
	case 9:
		return memcpy_fixed<9>(dest, src);
	case 10:
		return memcpy_fixed<10>(dest, src);
	case 11:
		return memcpy_fixed<11>(dest, src);
	case 12:
		return memcpy_fixed<12>(dest, src);
	case 13:
		return memcpy_fixed<13>(dest, src);
	case 14:
		return memcpy_fixed<14>(dest, src);
	case 15:
		return memcpy_fixed<15>(dest, src);
	case 16:
		return memcpy_fixed<16>(dest, src);
	case 17:
		return memcpy_fixed<17>(dest, src);
	case 18:
		return memcpy_fixed<18>(dest, src);
	case 19:
		return memcpy_fixed<19>(dest, src);
	case 20:
		return memcpy_fixed<20>(dest, src);
	case 21:
		return memcpy_fixed<21>(dest, src);
	case 22:
		return memcpy_fixed<22>(dest, src);
	case 23:
		return memcpy_fixed<23>(dest, src);
	case 24:
		return memcpy_fixed<24>(dest, src);
	case 25:
		return memcpy_fixed<25>(dest, src);
	case 26:
		return memcpy_fixed<26>(dest, src);
	case 27:
		return memcpy_fixed<27>(dest, src);
	case 28:
		return memcpy_fixed<28>(dest, src);
	case 29:
		return memcpy_fixed<29>(dest, src);
	case 30:
		return memcpy_fixed<30>(dest, src);
	case 31:
		return memcpy_fixed<31>(dest, src);
	case 32:
		return memcpy_fixed<32>(dest, src);
	case 33:
		return memcpy_fixed<33>(dest, src);
	case 34:
		return memcpy_fixed<34>(dest, src);
	case 35:
		return memcpy_fixed<35>(dest, src);
	case 36:
		return memcpy_fixed<36>(dest, src);
	case 37:
		return memcpy_fixed<37>(dest, src);
	case 38:
		return memcpy_fixed<38>(dest, src);
	case 39:
		return memcpy_fixed<39>(dest, src);
	case 40:
		return memcpy_fixed<40>(dest, src);
	case 41:
		return memcpy_fixed<41>(dest, src);
	case 42:
		return memcpy_fixed<42>(dest, src);
	case 43:
		return memcpy_fixed<43>(dest, src);
	case 44:
		return memcpy_fixed<44>(dest, src);
	case 45:
		return memcpy_fixed<45>(dest, src);
	case 46:
		return memcpy_fixed<46>(dest, src);
	case 47:
		return memcpy_fixed<47>(dest, src);
	case 48:
		return memcpy_fixed<48>(dest, src);
	case 49:
		return memcpy_fixed<49>(dest, src);
	case 50:
		return memcpy_fixed<50>(dest, src);
	case 51:
		return memcpy_fixed<51>(dest, src);
	case 52:
		return memcpy_fixed<52>(dest, src);
	case 53:
		return memcpy_fixed<53>(dest, src);
	case 54:
		return memcpy_fixed<54>(dest, src);
	case 55:
		return memcpy_fixed<55>(dest, src);
	case 56:
		return memcpy_fixed<56>(dest, src);
	case 57:
		return memcpy_fixed<57>(dest, src);
	case 58:
		return memcpy_fixed<58>(dest, src);
	case 59:
		return memcpy_fixed<59>(dest, src);
	case 60:
		return memcpy_fixed<60>(dest, src);
	case 61:
		return memcpy_fixed<61>(dest, src);
	case 62:
		return memcpy_fixed<62>(dest, src);
	case 63:
		return memcpy_fixed<63>(dest, src);
	case 64:
		return memcpy_fixed<64>(dest, src);
	case 65:
		return memcpy_fixed<65>(dest, src);
	case 66:
		return memcpy_fixed<66>(dest, src);
	case 67:
		return memcpy_fixed<67>(dest, src);
	case 68:
		return memcpy_fixed<68>(dest, src);
	case 69:
		return memcpy_fixed<69>(dest, src);
	case 70:
		return memcpy_fixed<70>(dest, src);
	case 71:
		return memcpy_fixed<71>(dest, src);
	case 72:
		return memcpy_fixed<72>(dest, src);
	case 73:
		return memcpy_fixed<73>(dest, src);
	case 74:
		return memcpy_fixed<74>(dest, src);
	case 75:
		return memcpy_fixed<75>(dest, src);
	case 76:
		return memcpy_fixed<76>(dest, src);
	case 77:
		return memcpy_fixed<77>(dest, src);
	case 78:
		return memcpy_fixed<78>(dest, src);
	case 79:
		return memcpy_fixed<79>(dest, src);
	case 80:
		return memcpy_fixed<80>(dest, src);
	case 81:
		return memcpy_fixed<81>(dest, src);
	case 82:
		return memcpy_fixed<82>(dest, src);
	case 83:
		return memcpy_fixed<83>(dest, src);
	case 84:
		return memcpy_fixed<84>(dest, src);
	case 85:
		return memcpy_fixed<85>(dest, src);
	case 86:
		return memcpy_fixed<86>(dest, src);
	case 87:
		return memcpy_fixed<87>(dest, src);
	case 88:
		return memcpy_fixed<88>(dest, src);
	case 89:
		return memcpy_fixed<89>(dest, src);
	case 90:
		return memcpy_fixed<90>(dest, src);
	case 91:
		return memcpy_fixed<91>(dest, src);
	case 92:
		return memcpy_fixed<92>(dest, src);
	case 93:
		return memcpy_fixed<93>(dest, src);
	case 94:
		return memcpy_fixed<94>(dest, src);
	case 95:
		return memcpy_fixed<95>(dest, src);
	case 96:
		return memcpy_fixed<96>(dest, src);
	case 97:
		return memcpy_fixed<97>(dest, src);
	case 98:
		return memcpy_fixed<98>(dest, src);
	case 99:
		return memcpy_fixed<99>(dest, src);
	case 100:
		return memcpy_fixed<100>(dest, src);
	case 101:
		return memcpy_fixed<101>(dest, src);
	case 102:
		return memcpy_fixed<102>(dest, src);
	case 103:
		return memcpy_fixed<103>(dest, src);
	case 104:
		return memcpy_fixed<104>(dest, src);
	case 105:
		return memcpy_fixed<105>(dest, src);
	case 106:
		return memcpy_fixed<106>(dest, src);
	case 107:
		return memcpy_fixed<107>(dest, src);
	case 108:
		return memcpy_fixed<108>(dest, src);
	case 109:
		return memcpy_fixed<109>(dest, src);
	case 110:
		return memcpy_fixed<110>(dest, src);
	case 111:
		return memcpy_fixed<111>(dest, src);
	case 112:
		return memcpy_fixed<112>(dest, src);
	case 113:
		return memcpy_fixed<113>(dest, src);
	case 114:
		return memcpy_fixed<114>(dest, src);
	case 115:
		return memcpy_fixed<115>(dest, src);
	case 116:
		return memcpy_fixed<116>(dest, src);
	case 117:
		return memcpy_fixed<117>(dest, src);
	case 118:
		return memcpy_fixed<118>(dest, src);
	case 119:
		return memcpy_fixed<119>(dest, src);
	case 120:
		return memcpy_fixed<120>(dest, src);
	case 121:
		return memcpy_fixed<121>(dest, src);
	case 122:
		return memcpy_fixed<122>(dest, src);
	case 123:
		return memcpy_fixed<123>(dest, src);
	case 124:
		return memcpy_fixed<124>(dest, src);
	case 125:
		return memcpy_fixed<125>(dest, src);
	case 126:
		return memcpy_fixed<126>(dest, src);
	case 127:
		return memcpy_fixed<127>(dest, src);
	case 128:
		return memcpy_fixed<128>(dest, src);
	case 129:
		return memcpy_fixed<129>(dest, src);
	case 130:
		return memcpy_fixed<130>(dest, src);
	case 131:
		return memcpy_fixed<131>(dest, src);
	case 132:
		return memcpy_fixed<132>(dest, src);
	case 133:
		return memcpy_fixed<133>(dest, src);
	case 134:
		return memcpy_fixed<134>(dest, src);
	case 135:
		return memcpy_fixed<135>(dest, src);
	case 136:
		return memcpy_fixed<136>(dest, src);
	case 137:
		return memcpy_fixed<137>(dest, src);
	case 138:
		return memcpy_fixed<138>(dest, src);
	case 139:
		return memcpy_fixed<139>(dest, src);
	case 140:
		return memcpy_fixed<140>(dest, src);
	case 141:
		return memcpy_fixed<141>(dest, src);
	case 142:
		return memcpy_fixed<142>(dest, src);
	case 143:
		return memcpy_fixed<143>(dest, src);
	case 144:
		return memcpy_fixed<144>(dest, src);
	case 145:
		return memcpy_fixed<145>(dest, src);
	case 146:
		return memcpy_fixed<146>(dest, src);
	case 147:
		return memcpy_fixed<147>(dest, src);
	case 148:
		return memcpy_fixed<148>(dest, src);
	case 149:
		return memcpy_fixed<149>(dest, src);
	case 150:
		return memcpy_fixed<150>(dest, src);
	case 151:
		return memcpy_fixed<151>(dest, src);
	case 152:
		return memcpy_fixed<152>(dest, src);
	case 153:
		return memcpy_fixed<153>(dest, src);
	case 154:
		return memcpy_fixed<154>(dest, src);
	case 155:
		return memcpy_fixed<155>(dest, src);
	case 156:
		return memcpy_fixed<156>(dest, src);
	case 157:
		return memcpy_fixed<157>(dest, src);
	case 158:
		return memcpy_fixed<158>(dest, src);
	case 159:
		return memcpy_fixed<159>(dest, src);
	case 160:
		return memcpy_fixed<160>(dest, src);
	case 161:
		return memcpy_fixed<161>(dest, src);
	case 162:
		return memcpy_fixed<162>(dest, src);
	case 163:
		return memcpy_fixed<163>(dest, src);
	case 164:
		return memcpy_fixed<164>(dest, src);
	case 165:
		return memcpy_fixed<165>(dest, src);
	case 166:
		return memcpy_fixed<166>(dest, src);
	case 167:
		return memcpy_fixed<167>(dest, src);
	case 168:
		return memcpy_fixed<168>(dest, src);
	case 169:
		return memcpy_fixed<169>(dest, src);
	case 170:
		return memcpy_fixed<170>(dest, src);
	case 171:
		return memcpy_fixed<171>(dest, src);
	case 172:
		return memcpy_fixed<172>(dest, src);
	case 173:
		return memcpy_fixed<173>(dest, src);
	case 174:
		return memcpy_fixed<174>(dest, src);
	case 175:
		return memcpy_fixed<175>(dest, src);
	case 176:
		return memcpy_fixed<176>(dest, src);
	case 177:
		return memcpy_fixed<177>(dest, src);
	case 178:
		return memcpy_fixed<178>(dest, src);
	case 179:
		return memcpy_fixed<179>(dest, src);
	case 180:
		return memcpy_fixed<180>(dest, src);
	case 181:
		return memcpy_fixed<181>(dest, src);
	case 182:
		return memcpy_fixed<182>(dest, src);
	case 183:
		return memcpy_fixed<183>(dest, src);
	case 184:
		return memcpy_fixed<184>(dest, src);
	case 185:
		return memcpy_fixed<185>(dest, src);
	case 186:
		return memcpy_fixed<186>(dest, src);
	case 187:
		return memcpy_fixed<187>(dest, src);
	case 188:
		return memcpy_fixed<188>(dest, src);
	case 189:
		return memcpy_fixed<189>(dest, src);
	case 190:
		return memcpy_fixed<190>(dest, src);
	case 191:
		return memcpy_fixed<191>(dest, src);
	case 192:
		return memcpy_fixed<192>(dest, src);
	case 193:
		return memcpy_fixed<193>(dest, src);
	case 194:
		return memcpy_fixed<194>(dest, src);
	case 195:
		return memcpy_fixed<195>(dest, src);
	case 196:
		return memcpy_fixed<196>(dest, src);
	case 197:
		return memcpy_fixed<197>(dest, src);
	case 198:
		return memcpy_fixed<198>(dest, src);
	case 199:
		return memcpy_fixed<199>(dest, src);
	case 200:
		return memcpy_fixed<200>(dest, src);
	case 201:
		return memcpy_fixed<201>(dest, src);
	case 202:
		return memcpy_fixed<202>(dest, src);
	case 203:
		return memcpy_fixed<203>(dest, src);
	case 204:
		return memcpy_fixed<204>(dest, src);
	case 205:
		return memcpy_fixed<205>(dest, src);
	case 206:
		return memcpy_fixed<206>(dest, src);
	case 207:
		return memcpy_fixed<207>(dest, src);
	case 208:
		return memcpy_fixed<208>(dest, src);
	case 209:
		return memcpy_fixed<209>(dest, src);
	case 210:
		return memcpy_fixed<210>(dest, src);
	case 211:
		return memcpy_fixed<211>(dest, src);
	case 212:
		return memcpy_fixed<212>(dest, src);
	case 213:
		return memcpy_fixed<213>(dest, src);
	case 214:
		return memcpy_fixed<214>(dest, src);
	case 215:
		return memcpy_fixed<215>(dest, src);
	case 216:
		return memcpy_fixed<216>(dest, src);
	case 217:
		return memcpy_fixed<217>(dest, src);
	case 218:
		return memcpy_fixed<218>(dest, src);
	case 219:
		return memcpy_fixed<219>(dest, src);
	case 220:
		return memcpy_fixed<220>(dest, src);
	case 221:
		return memcpy_fixed<221>(dest, src);
	case 222:
		return memcpy_fixed<222>(dest, src);
	case 223:
		return memcpy_fixed<223>(dest, src);
	case 224:
		return memcpy_fixed<224>(dest, src);
	case 225:
		return memcpy_fixed<225>(dest, src);
	case 226:
		return memcpy_fixed<226>(dest, src);
	case 227:
		return memcpy_fixed<227>(dest, src);
	case 228:
		return memcpy_fixed<228>(dest, src);
	case 229:
		return memcpy_fixed<229>(dest, src);
	case 230:
		return memcpy_fixed<230>(dest, src);
	case 231:
		return memcpy_fixed<231>(dest, src);
	case 232:
		return memcpy_fixed<232>(dest, src);
	case 233:
		return memcpy_fixed<233>(dest, src);
	case 234:
		return memcpy_fixed<234>(dest, src);
	case 235:
		return memcpy_fixed<235>(dest, src);
	case 236:
		return memcpy_fixed<236>(dest, src);
	case 237:
		return memcpy_fixed<237>(dest, src);
	case 238:
		return memcpy_fixed<238>(dest, src);
	case 239:
		return memcpy_fixed<239>(dest, src);
	case 240:
		return memcpy_fixed<240>(dest, src);
	case 241:
		return memcpy_fixed<241>(dest, src);
	case 242:
		return memcpy_fixed<242>(dest, src);
	case 243:
		return memcpy_fixed<243>(dest, src);
	case 244:
		return memcpy_fixed<244>(dest, src);
	case 245:
		return memcpy_fixed<245>(dest, src);
	case 246:
		return memcpy_fixed<246>(dest, src);
	case 247:
		return memcpy_fixed<247>(dest, src);
	case 248:
		return memcpy_fixed<248>(dest, src);
	case 249:
		return memcpy_fixed<249>(dest, src);
	case 250:
		return memcpy_fixed<250>(dest, src);
	case 251:
		return memcpy_fixed<251>(dest, src);
	case 252:
		return memcpy_fixed<252>(dest, src);
	case 253:
		return memcpy_fixed<253>(dest, src);
	case 254:
		return memcpy_fixed<254>(dest, src);
	case 255:
		return memcpy_fixed<255>(dest, src);
	case 256:
		return memcpy_fixed<256>(dest, src);
	default:
        memcpy_fixed<256>(dest, src);
        return fast_memcpy(reinterpret_cast<unsigned char *>(dest) + 256,
                           reinterpret_cast<const unsigned char *>(src) + 256, size - 256);
	}
}

//! This templated memcmp is significantly faster than std::memcmp,
//! but only when you are calling memcmp with a const size in a loop.
//! For instance `while (<cond>) { memcmp(<str1>, <str2>, const_size); ... }`
static inline int fast_memcmp(const void *str1, const void *str2, const size_t size) {
	switch (size) {
	case 0:
		return 0;
	case 1:
		return memcmp_fixed<1>(str1, str2);
	case 2:
		return memcmp_fixed<2>(str1, str2);
	case 3:
		return memcmp_fixed<3>(str1, str2);
	case 4:
		return memcmp_fixed<4>(str1, str2);
	case 5:
		return memcmp_fixed<5>(str1, str2);
	case 6:
		return memcmp_fixed<6>(str1, str2);
	case 7:
		return memcmp_fixed<7>(str1, str2);
	case 8:
		return memcmp_fixed<8>(str1, str2);
	case 9:
		return memcmp_fixed<9>(str1, str2);
	case 10:
		return memcmp_fixed<10>(str1, str2);
	case 11:
		return memcmp_fixed<11>(str1, str2);
	case 12:
		return memcmp_fixed<12>(str1, str2);
	case 13:
		return memcmp_fixed<13>(str1, str2);
	case 14:
		return memcmp_fixed<14>(str1, str2);
	case 15:
		return memcmp_fixed<15>(str1, str2);
	case 16:
		return memcmp_fixed<16>(str1, str2);
	case 17:
		return memcmp_fixed<17>(str1, str2);
	case 18:
		return memcmp_fixed<18>(str1, str2);
	case 19:
		return memcmp_fixed<19>(str1, str2);
	case 20:
		return memcmp_fixed<20>(str1, str2);
	case 21:
		return memcmp_fixed<21>(str1, str2);
	case 22:
		return memcmp_fixed<22>(str1, str2);
	case 23:
		return memcmp_fixed<23>(str1, str2);
	case 24:
		return memcmp_fixed<24>(str1, str2);
	case 25:
		return memcmp_fixed<25>(str1, str2);
	case 26:
		return memcmp_fixed<26>(str1, str2);
	case 27:
		return memcmp_fixed<27>(str1, str2);
	case 28:
		return memcmp_fixed<28>(str1, str2);
	case 29:
		return memcmp_fixed<29>(str1, str2);
	case 30:
		return memcmp_fixed<30>(str1, str2);
	case 31:
		return memcmp_fixed<31>(str1, str2);
	case 32:
		return memcmp_fixed<32>(str1, str2);
	case 33:
		return memcmp_fixed<33>(str1, str2);
	case 34:
		return memcmp_fixed<34>(str1, str2);
	case 35:
		return memcmp_fixed<35>(str1, str2);
	case 36:
		return memcmp_fixed<36>(str1, str2);
	case 37:
		return memcmp_fixed<37>(str1, str2);
	case 38:
		return memcmp_fixed<38>(str1, str2);
	case 39:
		return memcmp_fixed<39>(str1, str2);
	case 40:
		return memcmp_fixed<40>(str1, str2);
	case 41:
		return memcmp_fixed<41>(str1, str2);
	case 42:
		return memcmp_fixed<42>(str1, str2);
	case 43:
		return memcmp_fixed<43>(str1, str2);
	case 44:
		return memcmp_fixed<44>(str1, str2);
	case 45:
		return memcmp_fixed<45>(str1, str2);
	case 46:
		return memcmp_fixed<46>(str1, str2);
	case 47:
		return memcmp_fixed<47>(str1, str2);
	case 48:
		return memcmp_fixed<48>(str1, str2);
	case 49:
		return memcmp_fixed<49>(str1, str2);
	case 50:
		return memcmp_fixed<50>(str1, str2);
	case 51:
		return memcmp_fixed<51>(str1, str2);
	case 52:
		return memcmp_fixed<52>(str1, str2);
	case 53:
		return memcmp_fixed<53>(str1, str2);
	case 54:
		return memcmp_fixed<54>(str1, str2);
	case 55:
		return memcmp_fixed<55>(str1, str2);
	case 56:
		return memcmp_fixed<56>(str1, str2);
	case 57:
		return memcmp_fixed<57>(str1, str2);
	case 58:
		return memcmp_fixed<58>(str1, str2);
	case 59:
		return memcmp_fixed<59>(str1, str2);
	case 60:
		return memcmp_fixed<60>(str1, str2);
	case 61:
		return memcmp_fixed<61>(str1, str2);
	case 62:
		return memcmp_fixed<62>(str1, str2);
	case 63:
		return memcmp_fixed<63>(str1, str2);
	case 64:
		return memcmp_fixed<64>(str1, str2);
	case 65:
		return memcmp_fixed<65>(str1, str2);
	case 66:
		return memcmp_fixed<66>(str1, str2);
	case 67:
		return memcmp_fixed<67>(str1, str2);
	case 68:
		return memcmp_fixed<68>(str1, str2);
	case 69:
		return memcmp_fixed<69>(str1, str2);
	case 70:
		return memcmp_fixed<70>(str1, str2);
	case 71:
		return memcmp_fixed<71>(str1, str2);
	case 72:
		return memcmp_fixed<72>(str1, str2);
	case 73:
		return memcmp_fixed<73>(str1, str2);
	case 74:
		return memcmp_fixed<74>(str1, str2);
	case 75:
		return memcmp_fixed<75>(str1, str2);
	case 76:
		return memcmp_fixed<76>(str1, str2);
	case 77:
		return memcmp_fixed<77>(str1, str2);
	case 78:
		return memcmp_fixed<78>(str1, str2);
	case 79:
		return memcmp_fixed<79>(str1, str2);
	case 80:
		return memcmp_fixed<80>(str1, str2);
	case 81:
		return memcmp_fixed<81>(str1, str2);
	case 82:
		return memcmp_fixed<82>(str1, str2);
	case 83:
		return memcmp_fixed<83>(str1, str2);
	case 84:
		return memcmp_fixed<84>(str1, str2);
	case 85:
		return memcmp_fixed<85>(str1, str2);
	case 86:
		return memcmp_fixed<86>(str1, str2);
	case 87:
		return memcmp_fixed<87>(str1, str2);
	case 88:
		return memcmp_fixed<88>(str1, str2);
	case 89:
		return memcmp_fixed<89>(str1, str2);
	case 90:
		return memcmp_fixed<90>(str1, str2);
	case 91:
		return memcmp_fixed<91>(str1, str2);
	case 92:
		return memcmp_fixed<92>(str1, str2);
	case 93:
		return memcmp_fixed<93>(str1, str2);
	case 94:
		return memcmp_fixed<94>(str1, str2);
	case 95:
		return memcmp_fixed<95>(str1, str2);
	case 96:
		return memcmp_fixed<96>(str1, str2);
	case 97:
		return memcmp_fixed<97>(str1, str2);
	case 98:
		return memcmp_fixed<98>(str1, str2);
	case 99:
		return memcmp_fixed<99>(str1, str2);
	case 100:
		return memcmp_fixed<100>(str1, str2);
	case 101:
		return memcmp_fixed<101>(str1, str2);
	case 102:
		return memcmp_fixed<102>(str1, str2);
	case 103:
		return memcmp_fixed<103>(str1, str2);
	case 104:
		return memcmp_fixed<104>(str1, str2);
	case 105:
		return memcmp_fixed<105>(str1, str2);
	case 106:
		return memcmp_fixed<106>(str1, str2);
	case 107:
		return memcmp_fixed<107>(str1, str2);
	case 108:
		return memcmp_fixed<108>(str1, str2);
	case 109:
		return memcmp_fixed<109>(str1, str2);
	case 110:
		return memcmp_fixed<110>(str1, str2);
	case 111:
		return memcmp_fixed<111>(str1, str2);
	case 112:
		return memcmp_fixed<112>(str1, str2);
	case 113:
		return memcmp_fixed<113>(str1, str2);
	case 114:
		return memcmp_fixed<114>(str1, str2);
	case 115:
		return memcmp_fixed<115>(str1, str2);
	case 116:
		return memcmp_fixed<116>(str1, str2);
	case 117:
		return memcmp_fixed<117>(str1, str2);
	case 118:
		return memcmp_fixed<118>(str1, str2);
	case 119:
		return memcmp_fixed<119>(str1, str2);
	case 120:
		return memcmp_fixed<120>(str1, str2);
	case 121:
		return memcmp_fixed<121>(str1, str2);
	case 122:
		return memcmp_fixed<122>(str1, str2);
	case 123:
		return memcmp_fixed<123>(str1, str2);
	case 124:
		return memcmp_fixed<124>(str1, str2);
	case 125:
		return memcmp_fixed<125>(str1, str2);
	case 126:
		return memcmp_fixed<126>(str1, str2);
	case 127:
		return memcmp_fixed<127>(str1, str2);
	case 128:
		return memcmp_fixed<128>(str1, str2);
	case 129:
		return memcmp_fixed<129>(str1, str2);
	case 130:
		return memcmp_fixed<130>(str1, str2);
	case 131:
		return memcmp_fixed<131>(str1, str2);
	case 132:
		return memcmp_fixed<132>(str1, str2);
	case 133:
		return memcmp_fixed<133>(str1, str2);
	case 134:
		return memcmp_fixed<134>(str1, str2);
	case 135:
		return memcmp_fixed<135>(str1, str2);
	case 136:
		return memcmp_fixed<136>(str1, str2);
	case 137:
		return memcmp_fixed<137>(str1, str2);
	case 138:
		return memcmp_fixed<138>(str1, str2);
	case 139:
		return memcmp_fixed<139>(str1, str2);
	case 140:
		return memcmp_fixed<140>(str1, str2);
	case 141:
		return memcmp_fixed<141>(str1, str2);
	case 142:
		return memcmp_fixed<142>(str1, str2);
	case 143:
		return memcmp_fixed<143>(str1, str2);
	case 144:
		return memcmp_fixed<144>(str1, str2);
	case 145:
		return memcmp_fixed<145>(str1, str2);
	case 146:
		return memcmp_fixed<146>(str1, str2);
	case 147:
		return memcmp_fixed<147>(str1, str2);
	case 148:
		return memcmp_fixed<148>(str1, str2);
	case 149:
		return memcmp_fixed<149>(str1, str2);
	case 150:
		return memcmp_fixed<150>(str1, str2);
	case 151:
		return memcmp_fixed<151>(str1, str2);
	case 152:
		return memcmp_fixed<152>(str1, str2);
	case 153:
		return memcmp_fixed<153>(str1, str2);
	case 154:
		return memcmp_fixed<154>(str1, str2);
	case 155:
		return memcmp_fixed<155>(str1, str2);
	case 156:
		return memcmp_fixed<156>(str1, str2);
	case 157:
		return memcmp_fixed<157>(str1, str2);
	case 158:
		return memcmp_fixed<158>(str1, str2);
	case 159:
		return memcmp_fixed<159>(str1, str2);
	case 160:
		return memcmp_fixed<160>(str1, str2);
	case 161:
		return memcmp_fixed<161>(str1, str2);
	case 162:
		return memcmp_fixed<162>(str1, str2);
	case 163:
		return memcmp_fixed<163>(str1, str2);
	case 164:
		return memcmp_fixed<164>(str1, str2);
	case 165:
		return memcmp_fixed<165>(str1, str2);
	case 166:
		return memcmp_fixed<166>(str1, str2);
	case 167:
		return memcmp_fixed<167>(str1, str2);
	case 168:
		return memcmp_fixed<168>(str1, str2);
	case 169:
		return memcmp_fixed<169>(str1, str2);
	case 170:
		return memcmp_fixed<170>(str1, str2);
	case 171:
		return memcmp_fixed<171>(str1, str2);
	case 172:
		return memcmp_fixed<172>(str1, str2);
	case 173:
		return memcmp_fixed<173>(str1, str2);
	case 174:
		return memcmp_fixed<174>(str1, str2);
	case 175:
		return memcmp_fixed<175>(str1, str2);
	case 176:
		return memcmp_fixed<176>(str1, str2);
	case 177:
		return memcmp_fixed<177>(str1, str2);
	case 178:
		return memcmp_fixed<178>(str1, str2);
	case 179:
		return memcmp_fixed<179>(str1, str2);
	case 180:
		return memcmp_fixed<180>(str1, str2);
	case 181:
		return memcmp_fixed<181>(str1, str2);
	case 182:
		return memcmp_fixed<182>(str1, str2);
	case 183:
		return memcmp_fixed<183>(str1, str2);
	case 184:
		return memcmp_fixed<184>(str1, str2);
	case 185:
		return memcmp_fixed<185>(str1, str2);
	case 186:
		return memcmp_fixed<186>(str1, str2);
	case 187:
		return memcmp_fixed<187>(str1, str2);
	case 188:
		return memcmp_fixed<188>(str1, str2);
	case 189:
		return memcmp_fixed<189>(str1, str2);
	case 190:
		return memcmp_fixed<190>(str1, str2);
	case 191:
		return memcmp_fixed<191>(str1, str2);
	case 192:
		return memcmp_fixed<192>(str1, str2);
	case 193:
		return memcmp_fixed<193>(str1, str2);
	case 194:
		return memcmp_fixed<194>(str1, str2);
	case 195:
		return memcmp_fixed<195>(str1, str2);
	case 196:
		return memcmp_fixed<196>(str1, str2);
	case 197:
		return memcmp_fixed<197>(str1, str2);
	case 198:
		return memcmp_fixed<198>(str1, str2);
	case 199:
		return memcmp_fixed<199>(str1, str2);
	case 200:
		return memcmp_fixed<200>(str1, str2);
	case 201:
		return memcmp_fixed<201>(str1, str2);
	case 202:
		return memcmp_fixed<202>(str1, str2);
	case 203:
		return memcmp_fixed<203>(str1, str2);
	case 204:
		return memcmp_fixed<204>(str1, str2);
	case 205:
		return memcmp_fixed<205>(str1, str2);
	case 206:
		return memcmp_fixed<206>(str1, str2);
	case 207:
		return memcmp_fixed<207>(str1, str2);
	case 208:
		return memcmp_fixed<208>(str1, str2);
	case 209:
		return memcmp_fixed<209>(str1, str2);
	case 210:
		return memcmp_fixed<210>(str1, str2);
	case 211:
		return memcmp_fixed<211>(str1, str2);
	case 212:
		return memcmp_fixed<212>(str1, str2);
	case 213:
		return memcmp_fixed<213>(str1, str2);
	case 214:
		return memcmp_fixed<214>(str1, str2);
	case 215:
		return memcmp_fixed<215>(str1, str2);
	case 216:
		return memcmp_fixed<216>(str1, str2);
	case 217:
		return memcmp_fixed<217>(str1, str2);
	case 218:
		return memcmp_fixed<218>(str1, str2);
	case 219:
		return memcmp_fixed<219>(str1, str2);
	case 220:
		return memcmp_fixed<220>(str1, str2);
	case 221:
		return memcmp_fixed<221>(str1, str2);
	case 222:
		return memcmp_fixed<222>(str1, str2);
	case 223:
		return memcmp_fixed<223>(str1, str2);
	case 224:
		return memcmp_fixed<224>(str1, str2);
	case 225:
		return memcmp_fixed<225>(str1, str2);
	case 226:
		return memcmp_fixed<226>(str1, str2);
	case 227:
		return memcmp_fixed<227>(str1, str2);
	case 228:
		return memcmp_fixed<228>(str1, str2);
	case 229:
		return memcmp_fixed<229>(str1, str2);
	case 230:
		return memcmp_fixed<230>(str1, str2);
	case 231:
		return memcmp_fixed<231>(str1, str2);
	case 232:
		return memcmp_fixed<232>(str1, str2);
	case 233:
		return memcmp_fixed<233>(str1, str2);
	case 234:
		return memcmp_fixed<234>(str1, str2);
	case 235:
		return memcmp_fixed<235>(str1, str2);
	case 236:
		return memcmp_fixed<236>(str1, str2);
	case 237:
		return memcmp_fixed<237>(str1, str2);
	case 238:
		return memcmp_fixed<238>(str1, str2);
	case 239:
		return memcmp_fixed<239>(str1, str2);
	case 240:
		return memcmp_fixed<240>(str1, str2);
	case 241:
		return memcmp_fixed<241>(str1, str2);
	case 242:
		return memcmp_fixed<242>(str1, str2);
	case 243:
		return memcmp_fixed<243>(str1, str2);
	case 244:
		return memcmp_fixed<244>(str1, str2);
	case 245:
		return memcmp_fixed<245>(str1, str2);
	case 246:
		return memcmp_fixed<246>(str1, str2);
	case 247:
		return memcmp_fixed<247>(str1, str2);
	case 248:
		return memcmp_fixed<248>(str1, str2);
	case 249:
		return memcmp_fixed<249>(str1, str2);
	case 250:
		return memcmp_fixed<250>(str1, str2);
	case 251:
		return memcmp_fixed<251>(str1, str2);
	case 252:
		return memcmp_fixed<252>(str1, str2);
	case 253:
		return memcmp_fixed<253>(str1, str2);
	case 254:
		return memcmp_fixed<254>(str1, str2);
	case 255:
		return memcmp_fixed<255>(str1, str2);
	case 256:
		return memcmp_fixed<256>(str1, str2);
	default:
        return memcmp(str1, str2, size);
		// return memcmp_fixed<32>(str1, str2) + fast_memcmp(reinterpret_cast<const unsigned char *>(str1) + 32,
		//                                                   reinterpret_cast<const unsigned char *>(str2) + 32, size - 32);
	}
}

} // namespace duckdb
