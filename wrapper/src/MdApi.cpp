#include "ctp-rs/wrapper/include/MdApi.h"
#include "ctp-rs/wrapper/include/Converter.h"
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdio>

MdApi::MdApi(const MdSpi &gateway, rust::String flow_path, bool is_using_udp, bool is_multicast, bool is_production_mode) : gateway(gateway) {
    spi = new CMdSpi(this);
    api = CThostFtdcMdApi::CreateFtdcMdApi(flow_path.c_str(), is_using_udp, is_multicast, is_production_mode);
    api->RegisterSpi(spi);
}

MdApi::~MdApi() {
    if (api) {
        api->RegisterSpi(nullptr);  // Safe: just sets a pointer

        // Phase 4: Timeout-protected Release+Join to prevent CTP SDK exit()
        // CTP SDK Release()+Join() may call exit(1) when connection was never
        // established. Use a worker thread with 3s timeout to contain the risk.
        auto *api_ptr = api;
        std::atomic<bool> done{false};
        std::thread worker([api_ptr, &done]() {
            api_ptr->Release();
            api_ptr->Join();
            done.store(true, std::memory_order_release);
        });

        // Wait up to 3 seconds for Release+Join to complete
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                fprintf(stderr, "[CTP] MdApi::~MdApi: Release+Join timed out (3s), "
                                "detaching worker thread and leaking api to avoid exit()\n");
                worker.detach();
                // Leak api — do NOT set api = nullptr, do NOT delete
                // The worker thread may still be inside CTP SDK code;
                // leaking is safer than crashing the process.
                api = nullptr;  // Prevent double-free if destructor called again
                delete spi;
                spi = nullptr;
                return;  // Early return, skip normal cleanup below
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        worker.join();
        api = nullptr;
    }
    delete spi;
    spi = nullptr;
}

std::unique_ptr<MdApi> CreateMdApi(const MdSpi &gateway, rust::String flow_path, bool is_using_udp, bool is_multicast, bool is_production_mode) {
    return std::make_unique<MdApi>(gateway, flow_path, is_using_udp, is_multicast, is_production_mode);
}

rust::String MdApi::GetApiVersion() const {
    return api->GetApiVersion(
    );
}

void MdApi::Init() const {
    return api->Init(
    );
}

int32_t MdApi::Join() const {
    return api->Join(
    );
}

rust::String MdApi::GetTradingDay() const {
    return api->GetTradingDay(
    );
}

void MdApi::RegisterFront(rust::String pszFrontAddress) const {
    return api->RegisterFront(
        const_cast<char *>(pszFrontAddress.c_str())
    );
}

void MdApi::RegisterNameServer(rust::String pszNsAddress) const {
    return api->RegisterNameServer(
        const_cast<char *>(pszNsAddress.c_str())
    );
}

void MdApi::RegisterFensUserInfo(FensUserInfoField pFensUserInfo) const {
    CThostFtdcFensUserInfoField req(Converter::FensUserInfoFieldToCpp(pFensUserInfo));
    return api->RegisterFensUserInfo(
        &req
    );
}

int32_t MdApi::SubscribeMarketData(rust::Vec<rust::String> ppInstrumentID, int32_t nCount) const {
    char **ppInstrumentIDs = new char *[ppInstrumentID.size()];
    for (int i = 0; i < ppInstrumentID.size(); i++)
        ppInstrumentIDs[i] = (char *)ppInstrumentID[i].c_str();
    int ret = api->SubscribeMarketData(ppInstrumentIDs, ppInstrumentID.size());
    delete[] ppInstrumentIDs;
    return ret;
}

int32_t MdApi::UnSubscribeMarketData(rust::Vec<rust::String> ppInstrumentID, int32_t nCount) const {
    char **ppInstrumentIDs = new char *[ppInstrumentID.size()];
    for (int i = 0; i < ppInstrumentID.size(); i++)
        ppInstrumentIDs[i] = (char *)ppInstrumentID[i].c_str();
    int ret = api->UnSubscribeMarketData(ppInstrumentIDs, ppInstrumentID.size());
    delete[] ppInstrumentIDs;
    return ret;
}

int32_t MdApi::SubscribeForQuoteRsp(rust::Vec<rust::String> ppInstrumentID, int32_t nCount) const {
    char **ppInstrumentIDs = new char *[ppInstrumentID.size()];
    for (int i = 0; i < ppInstrumentID.size(); i++)
        ppInstrumentIDs[i] = (char *)ppInstrumentID[i].c_str();
    int ret = api->SubscribeForQuoteRsp(ppInstrumentIDs, ppInstrumentID.size());
    delete[] ppInstrumentIDs;
    return ret;
}

int32_t MdApi::UnSubscribeForQuoteRsp(rust::Vec<rust::String> ppInstrumentID, int32_t nCount) const {
    char **ppInstrumentIDs = new char *[ppInstrumentID.size()];
    for (int i = 0; i < ppInstrumentID.size(); i++)
        ppInstrumentIDs[i] = (char *)ppInstrumentID[i].c_str();
    int ret = api->UnSubscribeForQuoteRsp(ppInstrumentIDs, ppInstrumentID.size());
    delete[] ppInstrumentIDs;
    return ret;
}

int32_t MdApi::ReqUserLogin(ReqUserLoginField pReqUserLoginField, int32_t nRequestID) const {
    CThostFtdcReqUserLoginField req(Converter::ReqUserLoginFieldToCpp(pReqUserLoginField));
    return api->ReqUserLogin(
        &req,
        nRequestID
    );
}

int32_t MdApi::ReqUserLogout(UserLogoutField pUserLogout, int32_t nRequestID) const {
    CThostFtdcUserLogoutField req(Converter::UserLogoutFieldToCpp(pUserLogout));
    return api->ReqUserLogout(
        &req,
        nRequestID
    );
}

int32_t MdApi::ReqQryMulticastInstrument(QryMulticastInstrumentField pQryMulticastInstrument, int32_t nRequestID) const {
    CThostFtdcQryMulticastInstrumentField req(Converter::QryMulticastInstrumentFieldToCpp(pQryMulticastInstrument));
    return api->ReqQryMulticastInstrument(
        &req,
        nRequestID
    );
}
