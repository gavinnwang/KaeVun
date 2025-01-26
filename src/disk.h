#pragma once

#include "error.h"
#include "fd.h"
#include "freelist.h"
#include "mmap.h"
#include "os.h"
#include "page.h"
#include <cstdint>
#include <expected>
#include <fstream>
#include <mutex>
#include <sys/fcntl.h>
namespace kv {

class DiskHandler final : public PageHandler {
  static constexpr uint64_t INIT_MMAP_SIZE = 1 << 30;

public:
  DiskHandler() noexcept = default;
  [[nodiscard]] std::expected<uint64_t, Error>
  Open(std::filesystem::path path) noexcept {
    constexpr auto flags = (O_RDWR | O_CREAT);
    constexpr auto mode = 0666;
    LOG_TRACE("Opening db file: {}", path.string());

    // acquire a file descriptor
    auto fd = ::open(path.c_str(), flags, mode);
    if (fd == -1) {
      Close();
      LOG_ERROR("Failed to open db file");
      return std::unexpected{Error{"IO error"}};
    }
    fd_ = Fd{fd};
    path_ = path;
    page_size_ = OS::OSPageSize();

    // acquire file descriptor lock
    if (::flock(fd_.GetFd(), LOCK_EX) == -1) {
      LOG_ERROR("Failed to lock db file");
      Close();
      return std::unexpected{Error{"Failed to lock db file"}};
    }

    // open fstream for io
    fs_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!fs_.is_open()) {
      LOG_ERROR("Failed to open db file after creation: {}", path.string());
      Close();
      return std::unexpected{Error{"IO error"}};
    }
    fs_.exceptions(std::ios::goodbit);

    // set up mmap for io
    if (auto err_opt = mmap_handle_.Mmap(path_, fd_.GetFd(), INIT_MMAP_SIZE)) {
      return std::unexpected{*err_opt};
    }

    auto file_sz_or_err = OS::FileSize(path_);
    if (!file_sz_or_err) {
      Close();
      return std::unexpected{file_sz_or_err.error()};
    }

    auto file_sz = file_sz_or_err.value();
    opened_ = true;
    return file_sz;
  }

  // get page from mmap
  [[nodiscard]] Page &GetPage(Pgid id) noexcept final {
    assert(opened_);
    uint64_t pos = id * page_size_;

    assert(mmap_handle_.Valid() && pos + sizeof(Page) <= mmap_handle_.Size());

    LOG_INFO("Accessing mmap memory address: {}, page id: {}", GetAddress(pos),
             id);

    return *reinterpret_cast<Page *>(GetAddress(pos));
  }

  [[nodiscard]] void *GetAddress(uint64_t pos) const noexcept {
    return static_cast<std::byte *>(mmap_handle_.MmapPtr()) + pos;
  }

  [[nodiscard]] std::expected<PageBuffer, Error>
  CreatePageBufferFromDisk(uint32_t offset, uint32_t size) noexcept {
    assert(opened_);
    if (!fs_.is_open()) {
      return std::unexpected{Error{"Fs is not open"}};
    }

    if (fs_.exceptions() != std::ios::goodbit) {
      return std::unexpected{Error{"Fs exceptions must be disabled"}};
    }

    fs_.seekg(offset, std::ios::beg);
    if (!fs_) {
      return std::unexpected{Error{"Failed to seek to the offset"}};
    }

    PageBuffer buffer(size, page_size_);
    fs_.read(reinterpret_cast<char *>(buffer.GetBuffer().data()),
             size * page_size_);
    if (!fs_) {
      return std::unexpected{Error{"Failed to read data from disk"}};
    }

    return buffer;
  }

  void Close() noexcept {
    // assert(opened_);
    // release the mmap region to trigger the deconstructor that will unmap the
    // region
    if (fs_.is_open()) {
      fs_.close();
    }
    mmap_handle_.Reset();
    fd_.Reset();
  }

  [[nodiscard]] uint32_t PageSize() const noexcept {
    assert(opened_);
    return page_size_;
  }

  [[nodiscard]] std::optional<Error> WritePageBuffer(PageBuffer &buf,
                                                     Pgid start_pgid) noexcept {
    uint64_t offset = start_pgid * page_size_;
    fs_.seekp(offset);
    fs_.write(reinterpret_cast<char *>(buf.GetData().data()),
              buf.GetData().size());
    if (fs_.fail()) {
      return Error{"IO Error"};
    }
    return fd_.Sync();
  }

  std::optional<Error> Sync() const noexcept { return fd_.Sync(); }

  [[nodiscard]] std::expected<std::reference_wrapper<Page>, Error>
  Allocate(Meta rwtx_meta, uint32_t count) noexcept {
    PageBuffer buf{count, page_size_};
    auto &p = buf.GetPage(0);
    p.SetOverflow(count - 1);

    auto id_opt = freelist_.Allocate(count);
    // valid allocation
    if (id_opt.has_value()) {
      return p;
    }

    auto cur_wm = rwtx_meta.GetWatermark();
    p.SetId(cur_wm);
    assert(p.Id() > 3);
    auto min_sz = (p.Id() + count) * page_size_;
    if (min_sz > mmap_handle_.Size()) {
      auto err = mmap_handle_.Mmap(path_, fd_.GetFd(), min_sz);
      if (err) {
        return std::unexpected{*err};
      }
    }

    rwtx_meta.SetWatermark(cur_wm + count);
    return p;
  }

private:
  bool opened_{false};
  // path of the database file
  std::filesystem::path path_{""};
  // fstream of the database file
  std::fstream fs_;
  // file descriptor handle
  Fd fd_;
  // page size of the db
  uint32_t page_size_{OS::DEFAULT_PAGE_SIZE};
  // mutex to protect mmap access
  std::mutex mmaplock_;
  // mmap handle that will unmap when released
  MmapDataHandle mmap_handle_;
  // Freelist used to track reusable pages
  Freelist freelist_;
};

} // namespace kv
