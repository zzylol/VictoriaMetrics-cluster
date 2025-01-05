package promsketch

import (
	"fmt"
	"sync"

	"github.com/zzylol/go-kll"
)

type ExpoHistogramKLLOptimized struct {
	k                int64
	time_window_size int64
	kll_k            int

	s_count        int // sketch count
	arr_count      int
	max_array_size int
	min_array_size int
	array          []*EHArray

	klls       []*kll.Sketch
	max_time   []int64
	min_time   []int64
	bucketsize []int64

	mutex sync.Mutex
}

/*------------------------------------------------------------------------------
			Exponential Histogram for KLL
--------------------------------------------------------------------------------*/

func ExpoInitKLLOptimized(k int64, kll_k int, time_window_size int64) (ehkll *ExpoHistogramKLLOptimized) {
	ehkll = &ExpoHistogramKLLOptimized{
		k:                k,
		kll_k:            kll_k,
		s_count:          0,
		arr_count:        0,
		max_array_size:   0,
		min_array_size:   0, // not useful for kll because kll's memory is dynamic
		time_window_size: time_window_size,
		klls:             make([]*kll.Sketch, 0),
		array:            make([]*EHArray, 0),
	}

	return ehkll
}

func (ehkll *ExpoHistogramKLLOptimized) Update(time int64, value float64) {
	// remove expired buckets
	removed := 0
	for i := 0; i < ehkll.s_count; i++ {
		if ehkll.max_time[i] < time-ehkll.time_window_size {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		ehkll.s_count = ehkll.s_count - removed
		ehkll.klls = ehkll.klls[removed:]
		ehkll.max_time = ehkll.max_time[removed:]
		ehkll.min_time = ehkll.min_time[removed:]
		ehkll.bucketsize = ehkll.bucketsize[removed:]
	}

	removed = 0
	for i := 0; i < ehkll.arr_count; i++ {
		if ehkll.array[i].max_time < time-ehkll.time_window_size {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		ehkll.arr_count = ehkll.arr_count - removed
		ehkll.array = ehkll.array[removed:]
	}

	// add value to new EH bucket (array)
	if ehkll.arr_count > 0 && ehkll.array[ehkll.arr_count-1].bucket_size < ehkll.min_array_size {
		ehkll.array[ehkll.arr_count-1].samples = append(ehkll.array[ehkll.arr_count-1].samples, value)
		ehkll.array[ehkll.arr_count-1].max_time = time
		ehkll.array[ehkll.arr_count-1].bucket_size += 1
	} else {
		tmp_arr := NewArray()
		tmp_arr.samples = append(tmp_arr.samples, value)
		tmp_arr.max_time, tmp_arr.min_time = time, time
		tmp_arr.bucket_size = 1
		ehkll.array = append(ehkll.array, tmp_arr)
		ehkll.arr_count++
	}

	// Merge EH buckets (array)
	same_size_bucket := 1
	for i := ehkll.arr_count - 2; i >= 0; i-- {
		if ehkll.array[i].bucket_size == ehkll.array[i+1].bucket_size {
			same_size_bucket += 1
		} else {
			if float64(same_size_bucket) >= float64(ehkll.k)/2.0+2 {
				ehkll.array[i+1].MergeWith(ehkll.array[i+2])
				ehkll.array[i+1].bucket_size += ehkll.array[i+2].bucket_size
				ehkll.array[i+1].max_time = MaxInt64(ehkll.array[i+1].max_time, ehkll.array[i+2].max_time)
				ehkll.array[i+1].min_time = MinInt64(ehkll.array[i+1].min_time, ehkll.array[i+2].min_time)
				ehkll.array = append(ehkll.array[:i+2], ehkll.array[i+3:]...)
				ehkll.arr_count -= 1
			}
			same_size_bucket = 1
			if ehkll.array[i+1].bucket_size == ehkll.array[i].bucket_size {
				same_size_bucket += 1
			}
		}
	}

	if float64(same_size_bucket) >= float64(ehkll.k)/2.0+2 {
		ehkll.array[0].MergeWith(ehkll.array[1])
		ehkll.array[0].bucket_size += ehkll.array[1].bucket_size
		ehkll.array[0].max_time = MaxInt64(ehkll.array[0].max_time, ehkll.array[1].max_time)
		ehkll.array[0].min_time = MinInt64(ehkll.array[0].min_time, ehkll.array[1].min_time)
		ehkll.array = append(ehkll.array[:1], ehkll.array[2:]...)
		ehkll.arr_count -= 1
	}

	if ehkll.array[0].bucket_size >= ehkll.max_array_size {

		// init new KLL
		tmp := kll.New(ehkll.kll_k)
		ehkll.klls = append(ehkll.klls, tmp)
		ehkll.max_time = append(ehkll.max_time, ehkll.array[0].max_time)
		ehkll.min_time = append(ehkll.min_time, ehkll.array[0].min_time)
		ehkll.bucketsize = append(ehkll.bucketsize, int64(len(ehkll.array[0].samples)))
		for i := 0; i < len(ehkll.array[0].samples); i++ {
			ehkll.klls[ehkll.s_count].Update(ehkll.array[0].samples[i])
		}
		ehkll.s_count++

		ehkll.array = ehkll.array[1:]
		ehkll.arr_count -= 1

		// Merge EH buckets (KLL)
		same_size_bucket := 1
		for i := ehkll.s_count - 2; i >= 0; i-- {
			if ehkll.bucketsize[i] == ehkll.bucketsize[i+1] {
				same_size_bucket += 1
			} else {
				if float64(same_size_bucket) >= float64(ehkll.k)/2.0+2 {
					ehkll.klls[i+1].Merge(ehkll.klls[i+2])
					ehkll.bucketsize[i+1] += ehkll.bucketsize[i+2]
					ehkll.max_time[i+1] = MaxInt64(ehkll.max_time[i+1], ehkll.max_time[i+2])
					ehkll.min_time[i+1] = MinInt64(ehkll.min_time[i+1], ehkll.min_time[i+2])
					ehkll.klls = append(ehkll.klls[:i+2], ehkll.klls[i+3:]...)
					ehkll.max_time = append(ehkll.max_time[:i+2], ehkll.max_time[i+3:]...)
					ehkll.min_time = append(ehkll.min_time[:i+2], ehkll.min_time[i+3:]...)
					ehkll.bucketsize = append(ehkll.bucketsize[:i+2], ehkll.bucketsize[i+3:]...)
					ehkll.s_count -= 1
				}
				same_size_bucket = 1
				if ehkll.bucketsize[i+1] == ehkll.bucketsize[i] {
					same_size_bucket += 1
				}
			}
		}

		if float64(same_size_bucket) >= float64(ehkll.k)/2.0+2 {
			ehkll.klls[0].Merge(ehkll.klls[1])
			ehkll.bucketsize[0] += ehkll.bucketsize[1]
			ehkll.max_time[0] = MaxInt64(ehkll.max_time[0], ehkll.max_time[1])
			ehkll.min_time[0] = MinInt64(ehkll.min_time[0], ehkll.min_time[1])

			ehkll.klls = append(ehkll.klls[:1], ehkll.klls[2:]...)
			ehkll.max_time = append(ehkll.max_time[:1], ehkll.max_time[2:]...)
			ehkll.min_time = append(ehkll.min_time[:1], ehkll.min_time[2:]...)
			ehkll.bucketsize = append(ehkll.bucketsize[:1], ehkll.bucketsize[2:]...)
			ehkll.s_count -= 1
		}
	}
}

func (ehkll *ExpoHistogramKLLOptimized) Cover(mint, maxt int64) bool {
	if ehkll.s_count == 0 {
		return false
	}

	return (ehkll.array[ehkll.arr_count-1].max_time-ehkll.time_window_size <= mint)
}

func (ehkll *ExpoHistogramKLLOptimized) GetMemoryKB() float64 {
	var total_mem float64 = 0
	for i := 0; i < ehkll.s_count; i++ {
		total_mem += float64(ehkll.klls[i].GetSize()) * 8
	}
	for i := 0; i < ehkll.arr_count; i++ {
		total_mem += float64(len(ehkll.array[i].samples) * 8)
	}
	return total_mem / 1024 // KB
}

func (ehkll *ExpoHistogramKLLOptimized) print_buckets() {

	fmt.Println("k =", ehkll.k)
	fmt.Println("s_count =", ehkll.s_count)
	for i := 0; i < ehkll.s_count; i++ {
		fmt.Println(i, ehkll.min_time[i], ehkll.max_time[i], ehkll.bucketsize[i])
	}

	fmt.Println("arr_count =", ehkll.arr_count)
	for i := 0; i < ehkll.arr_count; i++ {
		fmt.Println(i, ehkll.array[i].min_time, ehkll.array[i].max_time, ehkll.array[i].bucket_size)
	}
}

func (ehkll *ExpoHistogramKLLOptimized) QueryIntervalMergeKLL(t1, t2 int64) (*kll.Sketch, *[]float64) {
	ehkll.mutex.Lock()
	if ehkll.s_count+ehkll.arr_count == 0 {
		ehkll.mutex.Unlock()
		return nil, nil
	}

	ehkll.print_buckets()
	fmt.Println()

	var from_bucket, to_bucket int = ehkll.s_count, ehkll.s_count

	for i := 0; i < ehkll.s_count; i++ {
		if t1 <= ehkll.max_time[i] && t1 >= ehkll.min_time[i] {
			from_bucket = i
		}
		if t2 <= ehkll.max_time[i] && t2 >= ehkll.min_time[i] {
			to_bucket = i
		}
	}

	for i := 0; i < ehkll.arr_count; i++ {
		if t1 >= ehkll.array[i].min_time && t1 <= ehkll.array[i].max_time {
			from_bucket = i + ehkll.s_count
		}
		if t2 >= ehkll.array[i].min_time && t2 <= ehkll.array[i].max_time {
			to_bucket = i + ehkll.s_count
		}
	}

	if ehkll.arr_count > 0 && t2 > ehkll.array[ehkll.arr_count-1].max_time {
		to_bucket = ehkll.arr_count - 1 + ehkll.s_count
	}
	if ehkll.s_count > 0 && t1 < ehkll.min_time[0] {
		from_bucket = 0
	}

	if from_bucket < ehkll.s_count {
		if AbsInt64(t1-ehkll.min_time[from_bucket]) > AbsInt64(t1-ehkll.max_time[from_bucket]) {
			from_bucket += 1
		}
	} else {
		if AbsInt64(t1-ehkll.array[from_bucket-ehkll.s_count].min_time) > AbsInt64(t1-ehkll.array[from_bucket-ehkll.s_count].max_time) {
			from_bucket += 1
		}
	}

	fmt.Println("s_count =", ehkll.s_count, "arr_count =", ehkll.arr_count, "total =", ehkll.s_count+ehkll.arr_count)
	fmt.Println("from_bucket =", from_bucket)
	fmt.Println("to_bucket =", to_bucket)

	if to_bucket < ehkll.s_count {
		if from_bucket < to_bucket {
			merged_kll := kll.New(ehkll.kll_k)
			for i := from_bucket; i <= to_bucket; i++ {
				merged_kll.Merge(ehkll.klls[i])
			}
			ehkll.mutex.Unlock()
			return merged_kll, nil
		} else {
			ehkll.mutex.Unlock()
			return ehkll.klls[from_bucket], nil
		}
	} else if from_bucket >= ehkll.s_count {
		// only in array part
		samples := make([]float64, 0)
		for i := from_bucket - ehkll.s_count; i <= to_bucket-ehkll.s_count; i++ {
			samples = append(samples, ehkll.array[i].samples...)
		}
		ehkll.mutex.Unlock()
		return nil, &samples
	} else {
		// merge kll and array
		merged_kll := kll.New(ehkll.kll_k)
		for i := from_bucket; i < ehkll.s_count; i++ {
			merged_kll.Merge(ehkll.klls[i])
		}

		for i := 0; i < to_bucket-ehkll.s_count; i++ {
			for j := 0; j < len(ehkll.array[i].samples); j++ {
				merged_kll.Update(ehkll.array[i].samples[j])
			}
		}
		ehkll.mutex.Unlock()
		return merged_kll, nil
	}
}
