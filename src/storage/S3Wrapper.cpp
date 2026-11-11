#include <conf/FaasmConfig.h>
#include <rados/librados.h>
#include <storage/S3Wrapper.h>

#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <errno.h>

#include <absl/container/flat_hash_map.h>

/*
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
*/

#include <mutex>
#include <shared_mutex>
#include <string_view>

/*
using namespace Aws::S3::Model;
using namespace Aws::Client;
using namespace Aws::Auth;
*/

namespace storage {

const char* FAASM_DEFAULT_POOL_NAME = "faasm";

file_not_found_error::file_not_found_error(const char* msg)
  : std::runtime_error(msg)
{
}
file_not_found_error::file_not_found_error(std::string msg)
  : std::runtime_error(msg)
{
}

/*
static Aws::SDKOptions options;

template<typename R>
R reqFactory(const std::string& bucket)
{
    R req;
    req.SetBucket(bucket);
    return req;
}

template<typename R>
R reqFactory(const std::string& bucket, const std::string& key)
{
    R req = reqFactory<R>(bucket);
    req.SetKey(key);
    return req;
}

void CHECK_ERRORS(const auto& response,
                  std::string_view bucketName,
                  std::string_view keyName)
{
    if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        if (std::string(bucketName).empty()) {
            SPDLOG_ERROR("General S3 error", bucketName);
        } else if (std::string(keyName).empty()) {
            SPDLOG_ERROR("S3 error with bucket: {}", bucketName);
        } else {
            SPDLOG_ERROR(
              "S3 error with bucket/key: {}/{}", bucketName, keyName);
        }
        SPDLOG_ERROR("S3 error: {}. {}",
                     err.GetExceptionName().c_str(),
                     err.GetMessage().c_str());
        if (response.GetError().GetErrorType() ==
            Aws::S3::S3Errors::NO_SUCH_KEY) {
            throw file_not_found_error(
              fmt::format("S3 key not found: {}/{}", bucketName, "/", keyName));
        } else {
            throw std::runtime_error("S3 error");
        }
    }
}

std::shared_ptr<AWSCredentialsProvider> getCredentialsProvider()
{
    return Aws::MakeShared<ProfileConfigFileAWSCredentialsProvider>("local");
}

ClientConfiguration getClientConf(long timeout)
{
    // There are a couple of conflicting pieces of info on how to configure
    // the AWS C++ SDK for use with minio:
    //
https://stackoverflow.com/questions/47105289/how-to-override-endpoint-in-aws-sdk-cpp-to-connect-to-minio-server-at-localhost
    // https://github.com/aws/aws-sdk-cpp/issues/587
    ClientConfiguration config;

    conf::FaasmConfig& faasmConf = conf::getFaasmConfig();

    config.region = "";
    config.verifySSL = false;
    config.endpointOverride = faasmConf.s3Host + ":" + faasmConf.s3Port;
    config.connectTimeoutMs = S3_CONNECT_TIMEOUT_MS;
    config.requestTimeoutMs = timeout;

    // Use HTTP, not HTTPS
    config.scheme = Aws::Http::Scheme::HTTP;

    return config;
}

*/

rados_t S3Wrapper::cluster = nullptr;

class RadosPool final
{
  public:
    RadosPool(std::string poolName)
    {
        std::scoped_lock<std::mutex> lock{ mx };
        rados_t& cluster = S3Wrapper::cluster;
        if (cluster == nullptr) {
            SPDLOG_CRITICAL(
              "Trying to open a pool without initializing storage first.");
            throw std::runtime_error(
              "Trying to open a pool without initializing storage first.");
        }
        int err = 0;
        err = rados_ioctx_create(cluster, poolName.c_str(), &this->ioctx);
        if (err < 0) {
            SPDLOG_CRITICAL("Couldn't open rados pool {}: {}",
                            FAASM_DEFAULT_POOL_NAME,
                            strerror(-err));
            throw new std::runtime_error("Couldn't open rados pool");
        }
    }

    ~RadosPool()
    {
        if (this->ioctx != nullptr) {
            rados_ioctx_destroy(this->ioctx);
            this->ioctx = nullptr;
        }
    }

    rados_ioctx_t ioctx = nullptr;
    std::mutex mx;
};

class RadosState final
{
  public:
    static RadosState& instance()
    {
        static RadosState state;
        return state;
    }

    std::shared_ptr<RadosPool> getPool(std::string name)
    {
        std::scoped_lock<std::mutex> lock{ poolMx };
        auto [it, created] = pools.try_emplace(name);
        if (created) {
            it->second = std::make_shared<RadosPool>(name);
        }
        return it->second;
    }

    void reset()
    {
        std::scoped_lock<std::mutex> lock{ poolMx };
        pools.clear();
    }

    void resetPool(std::string name)
    {
        std::scoped_lock<std::mutex> lock{ poolMx };
        pools.erase(name);
    }

  private:
    RadosState() { pools.reserve(32); }
    RadosState(RadosState&) = delete;
    RadosState& operator=(RadosState&) = delete;

    std::mutex poolMx;
    absl::flat_hash_map<std::string, std::shared_ptr<RadosPool>> pools;
};

void initFaasmS3()
{
    const auto& conf = conf::getFaasmConfig();
    SPDLOG_INFO(
      "Initialising Faasm librados setup at {}:{}", conf.s3Host, conf.s3Port);
    /*
    Aws::InitAPI(options);
    */
    rados_t& cluster = S3Wrapper::cluster;
    int err = 0;
    err = rados_create(&cluster, nullptr);
    if (err < 0) {
        cluster = nullptr;
        conf.print();
        SPDLOG_CRITICAL("Couldn't create the rados cluster handle: {}",
                        strerror(-err));
        throw new std::runtime_error(
          "Couldn't create the rados cluster handle");
    }
    SPDLOG_INFO("Created rados cluster handle.");

    err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
    if (err < 0) {
        SPDLOG_CRITICAL("Couldn't read config file at /etc/ceph/ceph.conf: {}",
                        strerror(-err));
        throw new std::runtime_error(
          "Couldn't read config file at /etc/ceph/ceph.conf");
    }

    err = rados_conf_parse_env(cluster, "CEPH_ARGS");
    if (err < 0) {
        SPDLOG_CRITICAL("Couldn't parse the CEPH_ARGS environment variable: {}",
                        strerror(-err));
        throw new std::runtime_error(
          "Couldn't parse the CEPH_ARGS environment variable");
    }

    err = rados_connect(cluster);
    if (err < 0) {
        SPDLOG_CRITICAL("Couldn't connect to the rados/ceph cluster: {}",
                        strerror(-err));
        throw new std::runtime_error(
          "Couldn't connect to the rados/ceph cluster");
    }

    S3Wrapper s3;
    s3.createBucket(conf.s3Bucket);
    // Check we can write/read
    s3.addKeyStr(conf.s3Bucket, "ping", "pong");
    std::string response = s3.getKeyStr(conf.s3Bucket, "ping");
    if (response != "pong") {
        std::string errorMsg =
          fmt::format("Unable to write/ read to/ from S3 ({})", response);
        SPDLOG_ERROR(errorMsg);
        throw std::runtime_error(errorMsg);
    }

    SPDLOG_INFO("Successfully pinged S3 at {}:{}", conf.s3Host, conf.s3Port);
}

void shutdownFaasmS3()
{
    RadosState::instance().reset();

    rados_t& cluster = S3Wrapper::cluster;
    if (cluster != nullptr) {
        rados_shutdown(cluster);
        cluster = nullptr;
    }
}

S3Wrapper::S3Wrapper()
  : faasmConf(conf::getFaasmConfig())
/*
  , clientConf(getClientConf(S3_REQUEST_TIMEOUT_MS))
  , client(AWSCredentials(faasmConf.s3User, faasmConf.s3Password),
           clientConf,
           AWSAuthV4Signer::PayloadSigningPolicy::Never,
           false)
*/
{
    //
}

void S3Wrapper::createBucket(const std::string& bucketName)
{
    SPDLOG_DEBUG("Creating bucket {}", bucketName);
    /*
    auto request = reqFactory<CreateBucketRequest>(bucketName);
    auto response = client.CreateBucket(request);

    if (!response.IsSuccess()) {
        const auto& err = response.GetError();

        auto errType = err.GetErrorType();
        if (errType == Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU ||
            errType == Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS) {
            SPDLOG_DEBUG("Bucket already exists {}", bucketName);
        } else {
            CHECK_ERRORS(response, bucketName, "");
        }
    }
    */
    int err = rados_pool_create(cluster, bucketName.c_str());
    if (err < 0) {
        if (err == -EEXIST) {
            SPDLOG_DEBUG("Bucket already exists {}", bucketName);
        } else {
            SPDLOG_ERROR(
              "Bucket {} cannot be created: {}", bucketName, strerror(-err));
            throw std::runtime_error("Bucket cannot be created.");
        }
    }
}

void S3Wrapper::deleteBucket(const std::string& bucketName)
{
    SPDLOG_DEBUG("Deleting bucket {}", bucketName);
    /*
    auto request = reqFactory<DeleteBucketRequest>(bucketName);
    auto response = client.DeleteBucket(request);

    if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        auto errType = err.GetErrorType();
        if (errType == Aws::S3::S3Errors::NO_SUCH_BUCKET) {
            SPDLOG_DEBUG("Bucket already deleted {}", bucketName);
        } else if (err.GetExceptionName() == "BucketNotEmpty") {
            SPDLOG_DEBUG("Bucket {} not empty, deleting keys", bucketName);
            std::vector<std::string> keys = listKeys(bucketName);
            for (const auto& k : keys) {
                deleteKey(bucketName, k);
            }

            // Recursively delete
            deleteBucket(bucketName);
        } else {
            CHECK_ERRORS(response, bucketName, "");
        }
    }
    */
    int err = rados_pool_delete(cluster, bucketName.c_str());
    if (err < 0) {
        if (err == -ENOENT) {
            SPDLOG_DEBUG("Bucket already doesn't exist {}", bucketName);
        } else {
            SPDLOG_ERROR(
              "Bucket {} cannot be destroyed: {}", bucketName, strerror(-err));
            throw std::runtime_error("Bucket cannot be destroyed.");
        }
    } else {
        RadosState::instance().resetPool(bucketName);
    }
}

std::vector<std::string> S3Wrapper::listBuckets()
{
    SPDLOG_TRACE("Listing buckets");
    /*
    auto response = client.ListBuckets();
    CHECK_ERRORS(response, "", "");

    Aws::Vector<Bucket> bucketObjects = response.GetResult().GetBuckets();

    std::vector<std::string> bucketNames;
    for (auto const& bucketObject : bucketObjects) {
        const Aws::String& awsStr = bucketObject.GetName();
        bucketNames.emplace_back(awsStr.c_str(), awsStr.size());
    }

    return bucketNames;
     */
    std::vector<std::string> names;
    size_t poolLen = rados_pool_list(cluster, nullptr, 0);
    std::vector<char> poolBuf(poolLen + 16, '\0');
    rados_pool_list(cluster, poolBuf.data(), poolBuf.size());
    poolBuf[poolBuf.size() - 2] = '\0';
    poolBuf[poolBuf.size() - 1] = '\0';
    ssize_t idx = 0;
    for (std::string name = std::string(poolBuf.data() + idx); !name.empty();) {
        idx += name.size() + 1;
        names.emplace_back(std::move(name));
        if (idx >= poolBuf.size()) {
            break;
        }
    }
    return names;
}

std::vector<std::string> S3Wrapper::listKeys(const std::string& bucketName)
{
    SPDLOG_TRACE("Listing keys in bucket {}", bucketName);
    /*
    auto request = reqFactory<ListObjectsRequest>(bucketName);
    auto response = client.ListObjects(request);

    std::vector<std::string> keys;
    if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        auto errType = err.GetErrorType();

        if (errType == Aws::S3::S3Errors::NO_SUCH_BUCKET) {
            SPDLOG_WARN("Listing keys of deleted bucket {}", bucketName);
            return keys;
        }

        CHECK_ERRORS(response, bucketName, "");
    }

    Aws::Vector<Object> keyObjects = response.GetResult().GetContents();
    if (keyObjects.empty()) {
        return keys;
    }

    for (auto const& keyObject : keyObjects) {
        const Aws::String& awsStr = keyObject.GetKey();
        keys.emplace_back(awsStr.c_str());
    }
    */
    std::vector<std::string> keys;
    auto pool = RadosState::instance().getPool(bucketName);
    rados_list_ctx_t lst = nullptr;
    int err = rados_nobjects_list_open(pool->ioctx, &lst);
    if (err < 0) {
        if (err == -ENOENT) {
            SPDLOG_WARN("Listing keys of deleted bucket {}", bucketName);
            return keys;
        }
        SPDLOG_ERROR(
          "Bucket {} cannot be listed: {}", bucketName, strerror(-err));
        throw std::runtime_error("Bucket cannot be listed.");
    }
    const char* entry;
    size_t entrySz;
    while (rados_nobjects_list_next2(
             lst, &entry, nullptr, nullptr, &entrySz, nullptr, nullptr) == 0) {
        keys.emplace_back(entry, entrySz);
    }
    rados_nobjects_list_close(lst);
    return keys;
}

void S3Wrapper::deleteKey(const std::string& bucketName,
                          const std::string& keyName)
{
    SPDLOG_TRACE("Deleting S3 key {}/{}", bucketName, keyName);
    /*
    auto request = reqFactory<DeleteObjectRequest>(bucketName, keyName);
    auto response = client.DeleteObject(request);

    if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        auto errType = err.GetErrorType();

        if (errType == Aws::S3::S3Errors::NO_SUCH_KEY) {
            SPDLOG_DEBUG("Key already deleted {}", keyName);
        } else if (errType == Aws::S3::S3Errors::NO_SUCH_BUCKET) {
            SPDLOG_DEBUG("Bucket already deleted {}", bucketName);
        } else {
            CHECK_ERRORS(response, bucketName, keyName);
        }
    }
    */
    auto pool = RadosState::instance().getPool(bucketName);
    int err = rados_remove(pool->ioctx, keyName.c_str());
    if (err < 0) {
        if (err == -ENOENT) {
            SPDLOG_DEBUG("Key already deleted {}", keyName);
        } else {
            SPDLOG_ERROR("Key {}/{} cannot be deleted: {}",
                         bucketName,
                         keyName,
                         strerror(-err));
            throw std::runtime_error("Key cannot be deleted.");
        }
    }
}

void S3Wrapper::addKeyBytes(const std::string& bucketName,
                            const std::string& keyName,
                            const std::span<const uint8_t> data)
{
    // See example:
    // https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/cpp/example_code/s3/put_object_buffer.cpp
    SPDLOG_TRACE("Writing S3 key {}/{} as bytes", bucketName, keyName);
    /*
    auto request = reqFactory<PutObjectRequest>(bucketName, keyName);

    const std::shared_ptr<Aws::IOStream> dataStream =
      Aws::MakeShared<Aws::StringStream>((char*)data.data());
    dataStream->write((char*)data.data(), data.size());
    dataStream->flush();

    request.SetBody(dataStream);

    auto response = client.PutObject(request);
    CHECK_ERRORS(response, bucketName, keyName);
     */
    if (rados_pool_lookup(cluster, bucketName.c_str()) == -ENOENT) {
        createBucket(bucketName);
    }
    auto pool = RadosState::instance().getPool(bucketName);
    int err = rados_write_full(pool->ioctx,
                               keyName.c_str(),
                               std::bit_cast<const char*>(data.data()),
                               data.size());
    if (err < 0) {
        SPDLOG_ERROR("Key {}/{} cannot be written: {}",
                     bucketName,
                     keyName,
                     strerror(-err));
        throw std::runtime_error("Key cannot be written.");
    }
}

void S3Wrapper::appendKeyBytes(const std::string& bucketName,
                               const std::string& keyName,
                               const std::span<const uint8_t> data)
{
    SPDLOG_TRACE("Appending S3 key {}/{} bytes", bucketName, keyName);

    if (rados_pool_lookup(cluster, bucketName.c_str()) == -ENOENT) {
        createBucket(bucketName);
    }
    auto pool = RadosState::instance().getPool(bucketName);
    int err = rados_append(pool->ioctx,
                           keyName.c_str(),
                           std::bit_cast<const char*>(data.data()),
                           data.size());
    if (err < 0) {
        SPDLOG_ERROR("Key {}/{} cannot be appended to: {}",
                     bucketName,
                     keyName,
                     strerror(-err));
        throw std::runtime_error("Key cannot be appended to.");
    }
}

void S3Wrapper::addKeyStr(const std::string& bucketName,
                          const std::string& keyName,
                          const std::string& data)
{
    // See example:
    // https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/cpp/example_code/s3/put_object_buffer.cpp
    SPDLOG_TRACE("Writing S3 key {}/{} as string", bucketName, keyName);
    /*
    auto request = reqFactory<PutObjectRequest>(bucketName, keyName);

    const std::shared_ptr<Aws::IOStream> dataStream =
      Aws::MakeShared<Aws::StringStream>("");
    *dataStream << data;
    dataStream->flush();

    request.SetBody(dataStream);
    auto response = client.PutObject(request);
    CHECK_ERRORS(response, bucketName, keyName);
     */
    auto pool = RadosState::instance().getPool(bucketName);
    int err =
      rados_write_full(pool->ioctx, keyName.c_str(), data.data(), data.size());
    if (err < 0) {
        SPDLOG_ERROR("Key {}/{} cannot be written: {}",
                     bucketName,
                     keyName,
                     strerror(-err));
        throw std::runtime_error("Key cannot be written.");
    }
}

std::vector<uint8_t> S3Wrapper::getKeyBytes(const std::string& bucketName,
                                            const std::string& keyName,
                                            bool tolerateMissing)
{
    SPDLOG_TRACE("Getting S3 key {}/{} as bytes", bucketName, keyName);
    /*
    auto request = reqFactory<GetObjectRequest>(bucketName, keyName);
    GetObjectOutcome response = client.GetObject(request);

    if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        auto errType = err.GetErrorType();

        if (tolerateMissing && (errType == Aws::S3::S3Errors::NO_SUCH_KEY)) {
            SPDLOG_TRACE(
              "Tolerating missing S3 key {}/{}", bucketName, keyName);
            std::vector<uint8_t> empty;
            return empty;
        }

        CHECK_ERRORS(response, bucketName, keyName);
    }

    std::vector<uint8_t> rawData(response.GetResult().GetContentLength());
    response.GetResult().GetBody().read((char*)rawData.data(), rawData.size());
    return rawData;
     */
    auto pool = RadosState::instance().getPool(bucketName);
    std::vector<uint8_t> data;
    uint64_t oSize;
    time_t mTime;
    int err = rados_stat(pool->ioctx, keyName.c_str(), &oSize, &mTime);
    if (err < 0) {
        if (err == -ENOENT && tolerateMissing) {
            return data;
        }
        SPDLOG_ERROR("Key {}/{} cannot be statted for getKeyBytes: {}, {}",
                     bucketName,
                     keyName,
                     strerror(-err),
                     tolerateMissing);
        throw std::runtime_error("Key cannot be statted.");
    }
    oSize = std::min(oSize, SIZE_MAX / 2);
    data.resize(static_cast<size_t>(oSize));
    err = rados_read(
      pool->ioctx, keyName.c_str(), (char*)data.data(), data.size(), 0);
    if (err < 0) {
        SPDLOG_ERROR(
          "Key {}/{} cannot be read: {}", bucketName, keyName, strerror(-err));
        throw std::runtime_error("Key cannot be read.");
    }
    data.resize(err);
    return data;
}

ssize_t S3Wrapper::getKeyPartIntoBuf(
  const std::string& bucketName,
  const std::string& keyName,
  ssize_t offset,
  ssize_t maxLength,
  std::function<void(ssize_t)> setBufferLength,
  std::function<char*()> getBuffer)
{
    SPDLOG_TRACE("Getting S3 key part {}/{}:{}+{} into string buffer",
                 bucketName,
                 keyName,
                 offset,
                 maxLength);
    auto pool = RadosState::instance().getPool(bucketName);
    uint64_t oSize;
    time_t mTime;
    int err = rados_stat(pool->ioctx, keyName.c_str(), &oSize, &mTime);
    if (err < 0) {
        SPDLOG_ERROR("Key {}/{} cannot be statted for getKeyPartIntoBuf: {}",
                     bucketName,
                     keyName,
                     strerror(-err));
        throw std::runtime_error("Key cannot be statted.");
    }
    oSize = std::min(oSize, SIZE_MAX / 2);

    // calculate slice
    oSize = std::max(ssize_t(0), std::min(ssize_t(oSize) - offset, maxLength));
    setBufferLength(static_cast<ssize_t>(oSize));
    char* buffer = getBuffer();
    if (oSize > 0) {
        // err = rados_read(pool->ioctx, keyName.c_str(), buffer, oSize, offset);
        size_t bytes_read;
        int prval;

        auto read_op = rados_create_read_op();
        rados_read_op_read(read_op, offset, oSize, buffer, &bytes_read, &prval);
        // rados_read_op_set_flags(read_op, LIBRADOS_OPERATION_BALANCE_READS);
        err = rados_read_op_operate(read_op,
                                    pool->ioctx,
                                    keyName.c_str(),
                                    LIBRADOS_OPERATION_BALANCE_READS);

        if (err < 0 || prval < 0) {
            SPDLOG_ERROR("Key {}/{} cannot be read: {}",
                         bucketName,
                         keyName,
                         strerror(-err));
            throw std::runtime_error("Key cannot be read.");
        }
        // return err;
        return bytes_read;
    }
    return 0;
}

std::string S3Wrapper::getKeyStr(const std::string& bucketName,
                                 const std::string& keyName)
{
    SPDLOG_TRACE("Getting S3 key {}/{} as string", bucketName, keyName);
    /*
    auto request = reqFactory<GetObjectRequest>(bucketName, keyName);
    GetObjectOutcome response = client.GetObject(request);
    CHECK_ERRORS(response, bucketName, keyName);

    std::ostringstream ss;
    auto* responseStream = response.GetResultWithOwnership().GetBody().rdbuf();
    ss << responseStream;

    return ss.str();
     */
    return faabric::util::bytesToString(
      getKeyBytes(bucketName, keyName, false));
}

int S3Wrapper::asyncNdpCall(const std::string& bucketName,
                                        const std::string& keyName,
                                        const std::string& funcClass,
                                        const std::string& funcName,
                                        std::span<const uint8_t> inputData,
                                        std::span<uint8_t> outputBuffer)
{
    SPDLOG_DEBUG(
      "Async Rados NDP call of {}:{} at {}/{} with {} bytes of input",
      funcClass,
      funcName,
      bucketName,
      keyName,
      inputData.size());
    auto pool = RadosState::instance().getPool(bucketName);

    if (pool->ioctx == nullptr) {
        SPDLOG_ERROR("Key {}/{} cannot be run: no pool", bucketName, keyName);
        throw std::runtime_error("Key cannot be run.");
    }

    rados_completion_t completion = nullptr;
    int ec =
      rados_aio_create_completion(nullptr, nullptr, nullptr, &completion);
    if (ec < 0) {
        SPDLOG_ERROR("Couldn't create a rados completion: {}", strerror(-ec));
        throw std::runtime_error("Couldn't create a rados completion");
    }
    ec = rados_aio_exec(pool->ioctx,
                        keyName.c_str(),
                        completion,
                        funcClass.c_str(),
                        funcName.c_str(),
                        reinterpret_cast<const char*>(inputData.data()),
                        inputData.size(),
                        reinterpret_cast<char*>(outputBuffer.data()),
                        outputBuffer.size());
    // int ec = rados_exec(pool->ioctx,
    //                     keyName.c_str(),
    //                     funcClass.c_str(),
    //                     funcName.c_str(),
    //                     reinterpret_cast<const char*>(inputData.data()),
    //                     inputData.size(),
    //                     reinterpret_cast<char*>(outputBuffer.data()),
    //                     outputBuffer.size());

    if (ec < 0) {
        rados_aio_release(completion);

        switch(ec)
        {
            case -ERANGE:
                SPDLOG_ERROR("[S3Wrapper.cpp] rados_exec failed with ERANGE. "
                             "Output buffer too small for {}:{}",
                             funcClass,
                             funcName);

                throw std::runtime_error("Output buffer too small for NDP call.");
            case -ENOENT:
                SPDLOG_ERROR("[S3Wrapper.cpp] rados_exec failed with ENOENT. Specified object does not exist.");
                throw file_not_found_error("Specified object does not exist.");
            case -EPERM:
                SPDLOG_ERROR("[S3Wrapper.cpp] The operation is not permitted, possibly due to insufficient permissions.");
                throw std::runtime_error("rados_exec failed with EPERM. The operation is not permitted, possibly due to insufficient permissions.");
            case -EIO:
                SPDLOG_ERROR("[S3Wrapper.cpp] rados_exec failed with EIO. An lower-level I/O Error occurred! Possibly with OSD or network");
                throw std::runtime_error("rados_exec failed with EIO. An I/O error occurred.");
            case -EINVAL:
                SPDLOG_ERROR("[S3Wrapper.cpp] rados_exec failed with EINVAL. Invalid argument passed to rados_exec");
                throw std::runtime_error("rados_exec failed with EINVAL. Invalid argument.");   
            default:
                SPDLOG_ERROR("[S3Wrapper.cpp] Key {}/{} cannot run {}:{}: {}",
                             bucketName,
                             keyName,
                             funcClass,
                             funcName,
                             strerror(-ec));
                throw std::runtime_error("Key cannot run an NDP call.");
        }
    }

    // rados_aio_exec only reports whether the op was submitted. The object
    // class call's own result (output length or -errno) is on the completion.
    // There is no timeout here: set `rados osd op timeout` in ceph.conf so a
    // stuck OSD can't hang this thread forever.
    rados_aio_wait_for_complete(completion);
    const int result = rados_aio_get_return_value(completion);
    rados_aio_release(completion);
    if (result < 0) {
        SPDLOG_ERROR("NDP call {}:{} on {}/{} failed: {}",
                     funcClass,
                     funcName,
                     bucketName,
                     keyName,
                     strerror(-result));
    }
    return result;
}
}
