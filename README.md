The issue is to count uniq lines in the file that doesn't fit in the memeory

Solution:\
We can't keep the whole file in the RAM. So we have to process the file line by line.
To be able to remove all the duplicates while processing the file line by line, we have to sort lines first.
Merge sort is the only way I can sort a file that doesn't fit in the memory.

Step by step:
1. Split the file into multiple chunks that fits in memory. Say 512Mb
2. Run sorting of each chunk concurrently (could be started as soon as the first chunk is written to file)
3. Merge files line by line to make a sorted file (could be started as soon as we have _mergeFactor_ chunks sorted)
   * Select Merge factor - how many files will be merged to a single file (e.g. 8)
   * Remove duplicates while merging
   * Remove merged files
5. Repeat until the final merging step (number of chunks <= merge factor)
6. On the final merging step it's enough just to count unique lines, we don't need to write the sorted result to a new file.




Run:\
Put file location to `inputPath` variable in main method and run the main method


configurations that works for me:
 - -xmx4g; sortChunkSize = 128L * Mb; sortParallelism = 6; mergeFactor = 8
 - -xmx6g; sortChunkSize = 256L * Mb; sortParallelism = 6; mergeFactor = 8
 
  
Test:\
Data: [120GB file](https://ecwid-vgv-storage.s3.eu-central-1.amazonaws.com/ip_addresses.zip) ; line size: 14 symbols average\
Hardware: MacBook Pro 2019, 2.6 GHz 6-Core, 16Gb RAM, SSD\
Params: -xmx4g; initialChunkSize = 128L * Mb; sortParallelism = 6; mergeFactor = 8\
Time: 1h 11m\
Result: 1_000_000_000
