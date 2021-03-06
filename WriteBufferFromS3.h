#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#    include <memory>
#    include <vector>
#    include <Core/Types.h>
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/HTTPCommon.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteBufferFromString.h>
#    include <aws/core/utils/memory/stl/AWSStringStream.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
const size_t subpart_size_for_last = 4 * 1024 * 1024;
const size_t subpart_size_for_merge = 64 * 1024 * 1024;
const size_t maximum_single_part_upload_size = 64 * 1024 * 1024;
/* Perform S3 HTTP PUT request.
 */
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    bool is_multipart;

    String bucket;
    String key;
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    size_t minimum_upload_part_size;
    std::shared_ptr<Aws::StringStream> temporary_buffer;
    size_t last_part_size;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String upload_id;
    std::vector<String> part_tags;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

public:
    explicit WriteBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t minimum_upload_part_size_,
        bool is_multipart,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalize() override;

    ~WriteBufferFromS3() override;

private:
    void initiate();
    void writePart();
    void writePartParallel(size_t subpart_size);
    void complete();
};

}

#endif
