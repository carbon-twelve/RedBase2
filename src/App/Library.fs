namespace Database

open System
open System.Runtime.InteropServices
open Microsoft.Win32.SafeHandles
open System.Threading
open System.IO
open System.Runtime.Serialization.Formatters.Binary
open System.Runtime.Caching

module Util =
    let undefined() = System.NotImplementedException() |> raise

module Interop =
    [<DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)>]
    extern SafeFileHandle CreateFile(
        [<MarshalAs(UnmanagedType.LPTStr)>] string filename,
        [<MarshalAs(UnmanagedType.U4)>] FileAccess access,
        [<MarshalAs(UnmanagedType.U4)>] FileShare share,
        IntPtr securityAttributes,
        [<MarshalAs(UnmanagedType.U4)>] FileMode creationDisposition,
        [<MarshalAs(UnmanagedType.U4)>] FileAttributes flagsAndAttributes,
        IntPtr templateFile
    )

    [<DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)>]    
    extern bool DeleteFile(
        [<MarshalAs(UnmanagedType.LPTStr)>] string fileName
    )

    [<DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)>]    
    extern bool WriteFile(
        SafeFileHandle safeFileHandle,
        byte[] buffer,
        uint32 numberOfByetsToWrite,
        uint32& numberOfByetsWritten,
        NativeOverlapped& overlapped
    )

/// <seealso cref="https://web.stanford.edu/class/cs346/2015/redbase-pf.html"/>
module PagedFile =

    let pfPageSize = 4092
    let pfBufferSize = 40 // The number of pages that the buffer pool may hold

    [<Struct>]
    type PageNum = { PageNum: uint64 }

    // TODO: implement this type
    type PageMetadata =
        static member Default: PageMetadata = Util.undefined()


    type IPFPageHandle =
        abstract member GetData: unit -> byte[]
        abstract member PageNum: PageNum
    
    type IPFFileHandle =
        inherit IDisposable
        abstract member GetFirstPage: unit -> IPFPageHandle
        abstract member GetLastPage: unit -> IPFPageHandle
        abstract member GetNextPage: current: PageNum -> IPFPageHandle
        abstract member GetPrevPage: current: PageNum -> IPFPageHandle
        abstract member GetThisPage: current: PageNum -> IPFPageHandle
        abstract member AllocatePage: unit -> unit
        abstract member DisposePage: pageNum: PageNum -> unit
        abstract member MarkDarty: pageNum: PageNum -> unit
        abstract member UnpinPage: pageNum: PageNum -> unit
        abstract member ForcePages: pageNum: PageNum -> unit

    type IPFManager =
        abstract member CreateFile: dirName: string -> unit
        abstract member DestroyFile: dirName: string -> unit
        abstract member OpenFile: dirName: string -> IPFFileHandle
        abstract member AllocateBlock: buffer: byte[] -> unit
        abstract member DisposeBlock: buffer: byte[] -> unit
    
    type PFPageHandle(data: byte[], pageNum: PageNum) =
        interface IPFPageHandle with
            member __.GetData(): byte[] = data
            member __.PageNum: PageNum = pageNum

    // This class is immutable
    type FileMetadata(dbDirectory: DirectoryInfo, pageMap: Map<PageNum, PageMetadata>) =

        new (dbDirectory) = FileMetadata(dbDirectory, Map.empty)

        static member Parse(buffer: byte[], offset: int, count: int): FileMetadata =
            let stream = new MemoryStream(buffer, offset, count)
            let formatter = BinaryFormatter()
            formatter.Deserialize(stream) :?> FileMetadata

        member this.Persist(): unit =
            use tempFileStream = new FileStream(Path.Combine(dbDirectory.FullName, "metadata.tmp"), FileMode.CreateNew)
            let formatter = BinaryFormatter()
            formatter.Serialize(tempFileStream, this)
            tempFileStream.Dispose()
            File.Replace(Path.Combine(dbDirectory.FullName, "metadata.tmp"), Path.Combine(dbDirectory.FullName, "metadata"), Path.Combine(dbDirectory.FullName, "metadata.bak"))

        member __.PageCount = pageMap.Count

        member this.AllocatePage(): FileMetadata =
            FileMetadata(dbDirectory, Map.add { PageNum = (uint64) this.PageCount + 1UL } PageMetadata.Default pageMap)

    // TODO: implement this class
    type BufferPool() =
        inherit ObjectCache()
        


    /// <p>This object represents a heap file</p>
    /// <param name="fileStream">The underlying FileStream. It is owned by this object, hence no other object should call Dispose on it</param>
    type PFFileHandle(contentFile: FileStream, buffer: BufferPool, initialMetadata: FileMetadata) =

        let metadataLock = Object()
        let mutable metadata: FileMetadata = initialMetadata

        static member Create(dbDirectory: DirectoryInfo): PFFileHandle =
            // Just confrim the metadata file can be created
            (new FileStream(Path.Combine(dbDirectory.FullName, "metadata"), FileMode.CreateNew)).Dispose()
            let contentFile = new FileStream(Path.Combine(dbDirectory.FullName, "content"), FileMode.CreateNew)
            let buffer = new BufferPool()
            new PFFileHandle(contentFile, buffer, FileMetadata(dbDirectory))

        interface IPFFileHandle with

            // This does nothing with the underlying file but allocates a new page in the buffer pool instead
            member __.AllocatePage(): unit =
                lock metadataLock begin fun _ ->
                    let pageNum = { PageNum = (uint64) metadata.PageCount }
                    let pageHandle = PFPageHandle(Array.zeroCreate pfPageSize, pageNum)
                    let policy = new CacheItemPolicy()
                    policy.Priority <- CacheItemPriority.NotRemovable
                    buffer.Add(new CacheItem(pageNum.ToString(), pageHandle), policy) |> ignore
                    metadata <- metadata.AllocatePage()
                end

            member this.Dispose(): unit = contentFile.Dispose() // FIXME: Flush all dirty pages in the buffer pool

            member this.DisposePage(pageNum: PageNum): unit = 
                raise (System.NotImplementedException())
            member this.ForcePages(pageNum: PageNum): unit = 
                raise (System.NotImplementedException())
            member this.GetFirstPage(): IPFPageHandle = Util.undefined()
            member this.GetLastPage(): IPFPageHandle = 
                raise (System.NotImplementedException())
            member this.GetNextPage(current: PageNum): IPFPageHandle = 
                raise (System.NotImplementedException())
            member this.GetPrevPage(current: PageNum): IPFPageHandle = 
                raise (System.NotImplementedException())
            member this.GetThisPage(current: PageNum): IPFPageHandle = 
                raise (System.NotImplementedException())
            member this.MarkDarty(pageNum: PageNum): unit = 
                raise (System.NotImplementedException())
            member this.UnpinPage(pageNum: PageNum): unit = 
                raise (System.NotImplementedException())
            
    type PFManager() =
        
        interface IPFManager with

            member __.AllocateBlock(_: byte []): unit = Util.undefined()
                
            member __.CreateFile(fileName: string): unit =
                let fileStream = new FileStream(fileName, FileMode.CreateNew)
                fileStream.Dispose()

            member __.DestroyFile(fileName: string): unit = File.Delete(fileName)

            member __.DisposeBlock(_: byte []): unit = Util.undefined()

            member __.OpenFile(dirName: string): IPFFileHandle =
                new FileStream(dirName, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 4096, true)
                |> (fun h -> new PFFileHandle(h) :> IPFFileHandle)

module Main =
    open PagedFile

    [<EntryPoint>]
    let main argv =
        let handle = Interop.CreateFile("test.db", FileAccess.ReadWrite, FileShare.None, IntPtr.Zero, FileMode.Open, FileAttributes.Normal, IntPtr.Zero)   
        let mutable n = 0u
        let mutable overlapped = NativeOverlapped()
        let success = Interop.WriteFile(handle, [| 1uy |], 1u, &n, &overlapped)
        printf "%A,%A,%A" n overlapped success
        0
