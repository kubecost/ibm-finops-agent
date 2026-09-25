package adapters

// Pin stands in for the pinning the fix adds: today's adapters have nothing to pin, so a
// computation's reads are only as consistent as the individual adapter locks make them.
func (ocdsa *OpenCostDataSourceAdapter) Pin() (release func()) { return func() {} }
