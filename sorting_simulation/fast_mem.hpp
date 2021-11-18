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
	case 4:
		return memcpy_fixed<4>(dest, src);
	case 8:
		return memcpy_fixed<8>(dest, src);
	case 12:
		return memcpy_fixed<12>(dest, src);
	case 16:
		return memcpy_fixed<16>(dest, src);
	case 20:
		return memcpy_fixed<20>(dest, src);
	case 24:
		return memcpy_fixed<24>(dest, src);
	case 28:
		return memcpy_fixed<28>(dest, src);
	case 32:
		return memcpy_fixed<32>(dest, src);
	case 36:
		return memcpy_fixed<36>(dest, src);
	case 40:
		return memcpy_fixed<40>(dest, src);
	case 44:
		return memcpy_fixed<44>(dest, src);
	case 48:
		return memcpy_fixed<48>(dest, src);
	case 52:
		return memcpy_fixed<52>(dest, src);
	case 56:
		return memcpy_fixed<56>(dest, src);
	case 60:
		return memcpy_fixed<60>(dest, src);
	case 64:
		return memcpy_fixed<64>(dest, src);
	case 68:
		return memcpy_fixed<68>(dest, src);
	case 72:
		return memcpy_fixed<72>(dest, src);
	case 76:
		return memcpy_fixed<76>(dest, src);
	case 80:
		return memcpy_fixed<80>(dest, src);
	case 84:
		return memcpy_fixed<84>(dest, src);
	case 88:
		return memcpy_fixed<88>(dest, src);
	case 92:
		return memcpy_fixed<92>(dest, src);
	case 96:
		return memcpy_fixed<96>(dest, src);
	case 100:
		return memcpy_fixed<100>(dest, src);
	case 104:
		return memcpy_fixed<104>(dest, src);
	case 108:
		return memcpy_fixed<108>(dest, src);
	case 112:
		return memcpy_fixed<112>(dest, src);
	case 116:
		return memcpy_fixed<116>(dest, src);
	case 120:
		return memcpy_fixed<120>(dest, src);
	case 124:
		return memcpy_fixed<124>(dest, src);
	case 128:
		return memcpy_fixed<128>(dest, src);
	case 132:
		return memcpy_fixed<132>(dest, src);
	case 136:
		return memcpy_fixed<136>(dest, src);
	case 140:
		return memcpy_fixed<140>(dest, src);
	case 144:
		return memcpy_fixed<144>(dest, src);
	case 148:
		return memcpy_fixed<148>(dest, src);
	case 152:
		return memcpy_fixed<152>(dest, src);
	case 156:
		return memcpy_fixed<156>(dest, src);
	case 160:
		return memcpy_fixed<160>(dest, src);
	case 164:
		return memcpy_fixed<164>(dest, src);
	case 168:
		return memcpy_fixed<168>(dest, src);
	case 172:
		return memcpy_fixed<172>(dest, src);
	case 176:
		return memcpy_fixed<176>(dest, src);
	case 180:
		return memcpy_fixed<180>(dest, src);
	case 184:
		return memcpy_fixed<184>(dest, src);
	case 188:
		return memcpy_fixed<188>(dest, src);
	case 192:
		return memcpy_fixed<192>(dest, src);
	case 196:
		return memcpy_fixed<196>(dest, src);
	case 200:
		return memcpy_fixed<200>(dest, src);
	case 204:
		return memcpy_fixed<204>(dest, src);
	case 208:
		return memcpy_fixed<208>(dest, src);
	case 212:
		return memcpy_fixed<212>(dest, src);
	case 216:
		return memcpy_fixed<216>(dest, src);
	case 220:
		return memcpy_fixed<220>(dest, src);
	case 224:
		return memcpy_fixed<224>(dest, src);
	case 228:
		return memcpy_fixed<228>(dest, src);
	case 232:
		return memcpy_fixed<232>(dest, src);
	case 236:
		return memcpy_fixed<236>(dest, src);
	case 240:
		return memcpy_fixed<240>(dest, src);
	case 244:
		return memcpy_fixed<244>(dest, src);
	case 248:
		return memcpy_fixed<248>(dest, src);
	case 252:
		return memcpy_fixed<252>(dest, src);
	case 256:
		return memcpy_fixed<256>(dest, src);
	// case 260:
	// 	return memcpy_fixed<260>(dest, src);
	// case 264:
	// 	return memcpy_fixed<264>(dest, src);
	// case 268:
	// 	return memcpy_fixed<268>(dest, src);
	// case 272:
	// 	return memcpy_fixed<272>(dest, src);
	// case 276:
	// 	return memcpy_fixed<276>(dest, src);
	// case 280:
	// 	return memcpy_fixed<280>(dest, src);
	// case 284:
	// 	return memcpy_fixed<284>(dest, src);
	// case 288:
	// 	return memcpy_fixed<288>(dest, src);
	// case 292:
	// 	return memcpy_fixed<292>(dest, src);
	// case 296:
	// 	return memcpy_fixed<296>(dest, src);
	// case 300:
	// 	return memcpy_fixed<300>(dest, src);
	// case 304:
	// 	return memcpy_fixed<304>(dest, src);
	// case 308:
	// 	return memcpy_fixed<308>(dest, src);
	// case 312:
	// 	return memcpy_fixed<312>(dest, src);
	// case 316:
	// 	return memcpy_fixed<316>(dest, src);
	// case 320:
	// 	return memcpy_fixed<320>(dest, src);
	// case 324:
	// 	return memcpy_fixed<324>(dest, src);
	// case 328:
	// 	return memcpy_fixed<328>(dest, src);
	// case 332:
	// 	return memcpy_fixed<332>(dest, src);
	// case 336:
	// 	return memcpy_fixed<336>(dest, src);
	// case 340:
	// 	return memcpy_fixed<340>(dest, src);
	// case 344:
	// 	return memcpy_fixed<344>(dest, src);
	// case 348:
	// 	return memcpy_fixed<348>(dest, src);
	// case 352:
	// 	return memcpy_fixed<352>(dest, src);
	// case 356:
	// 	return memcpy_fixed<356>(dest, src);
	// case 360:
	// 	return memcpy_fixed<360>(dest, src);
	// case 364:
	// 	return memcpy_fixed<364>(dest, src);
	// case 368:
	// 	return memcpy_fixed<368>(dest, src);
	// case 372:
	// 	return memcpy_fixed<372>(dest, src);
	// case 376:
	// 	return memcpy_fixed<376>(dest, src);
	// case 380:
	// 	return memcpy_fixed<380>(dest, src);
	// case 384:
	// 	return memcpy_fixed<384>(dest, src);
	// case 388:
	// 	return memcpy_fixed<388>(dest, src);
	// case 392:
	// 	return memcpy_fixed<392>(dest, src);
	// case 396:
	// 	return memcpy_fixed<396>(dest, src);
	// case 400:
	// 	return memcpy_fixed<400>(dest, src);
	// case 404:
	// 	return memcpy_fixed<404>(dest, src);
	// case 408:
	// 	return memcpy_fixed<408>(dest, src);
	// case 412:
	// 	return memcpy_fixed<412>(dest, src);
	// case 416:
	// 	return memcpy_fixed<416>(dest, src);
	// case 420:
	// 	return memcpy_fixed<420>(dest, src);
	// case 424:
	// 	return memcpy_fixed<424>(dest, src);
	// case 428:
	// 	return memcpy_fixed<428>(dest, src);
	// case 432:
	// 	return memcpy_fixed<432>(dest, src);
	// case 436:
	// 	return memcpy_fixed<436>(dest, src);
	// case 440:
	// 	return memcpy_fixed<440>(dest, src);
	// case 444:
	// 	return memcpy_fixed<444>(dest, src);
	// case 448:
	// 	return memcpy_fixed<448>(dest, src);
	// case 452:
	// 	return memcpy_fixed<452>(dest, src);
	// case 456:
	// 	return memcpy_fixed<456>(dest, src);
	// case 460:
	// 	return memcpy_fixed<460>(dest, src);
	// case 464:
	// 	return memcpy_fixed<464>(dest, src);
	// case 468:
	// 	return memcpy_fixed<468>(dest, src);
	// case 472:
	// 	return memcpy_fixed<472>(dest, src);
	// case 476:
	// 	return memcpy_fixed<476>(dest, src);
	// case 480:
	// 	return memcpy_fixed<480>(dest, src);
	// case 484:
	// 	return memcpy_fixed<484>(dest, src);
	// case 488:
	// 	return memcpy_fixed<488>(dest, src);
	// case 492:
	// 	return memcpy_fixed<492>(dest, src);
	// case 496:
	// 	return memcpy_fixed<496>(dest, src);
	// case 500:
	// 	return memcpy_fixed<500>(dest, src);
	// case 504:
	// 	return memcpy_fixed<504>(dest, src);
	// case 508:
	// 	return memcpy_fixed<508>(dest, src);
	// case 512:
	// 	return memcpy_fixed<512>(dest, src);
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
	// case 33:
	// 	return memcmp_fixed<33>(str1, str2);
	// case 34:
	// 	return memcmp_fixed<34>(str1, str2);
	// case 35:
	// 	return memcmp_fixed<35>(str1, str2);
	// case 36:
	// 	return memcmp_fixed<36>(str1, str2);
	// case 37:
	// 	return memcmp_fixed<37>(str1, str2);
	// case 38:
	// 	return memcmp_fixed<38>(str1, str2);
	// case 39:
	// 	return memcmp_fixed<39>(str1, str2);
	// case 40:
	// 	return memcmp_fixed<40>(str1, str2);
	// case 41:
	// 	return memcmp_fixed<41>(str1, str2);
	// case 42:
	// 	return memcmp_fixed<42>(str1, str2);
	// case 43:
	// 	return memcmp_fixed<43>(str1, str2);
	// case 44:
	// 	return memcmp_fixed<44>(str1, str2);
	// case 45:
	// 	return memcmp_fixed<45>(str1, str2);
	// case 46:
	// 	return memcmp_fixed<46>(str1, str2);
	// case 47:
	// 	return memcmp_fixed<47>(str1, str2);
	// case 48:
	// 	return memcmp_fixed<48>(str1, str2);
	// case 49:
	// 	return memcmp_fixed<49>(str1, str2);
	// case 50:
	// 	return memcmp_fixed<50>(str1, str2);
	// case 51:
	// 	return memcmp_fixed<51>(str1, str2);
	// case 52:
	// 	return memcmp_fixed<52>(str1, str2);
	// case 53:
	// 	return memcmp_fixed<53>(str1, str2);
	// case 54:
	// 	return memcmp_fixed<54>(str1, str2);
	// case 55:
	// 	return memcmp_fixed<55>(str1, str2);
	// case 56:
	// 	return memcmp_fixed<56>(str1, str2);
	// case 57:
	// 	return memcmp_fixed<57>(str1, str2);
	// case 58:
	// 	return memcmp_fixed<58>(str1, str2);
	// case 59:
	// 	return memcmp_fixed<59>(str1, str2);
	// case 60:
	// 	return memcmp_fixed<60>(str1, str2);
	// case 61:
	// 	return memcmp_fixed<61>(str1, str2);
	// case 62:
	// 	return memcmp_fixed<62>(str1, str2);
	// case 63:
	// 	return memcmp_fixed<63>(str1, str2);
	// case 64:
	// 	return memcmp_fixed<64>(str1, str2);
	default:
        return memcmp(str1, str2, size);
		// return memcmp_fixed<32>(str1, str2) + fast_memcmp(reinterpret_cast<const unsigned char *>(str1) + 32,
		//                                                   reinterpret_cast<const unsigned char *>(str2) + 32, size - 32);
	}
}

} // namespace duckdb
