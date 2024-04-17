package hashtable

const (
	kInitialCellCntBits = 10
	kInitialCellCnt     = 1 << kInitialCellCntBits

	kLoadFactorNumerator   = 1
	kLoadFactorDenominator = 2

	//kTwoLevelBucketCntBits = 8
	//kTwoLevelBucketCnt     = 1 << kTwoLevelBucketCntBits
	//kMaxTwoLevelBucketCnt  = kTwoLevelBucketCnt - 1
)
