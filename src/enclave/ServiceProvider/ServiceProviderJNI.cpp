#include "SP.h"

#include <cstdint>
#include <cstdio>

#include "ServiceProvider.h"

/**
 * Throw a Java exception with the specified message.
 *
 * Important: Note that this function will return to the caller. The exception is only thrown at the
 * end of the JNI method invocation.
 */
void jni_throw(JNIEnv *env, const char *message) {
  jclass exception = env->FindClass("edu/berkeley/cs/rise/opaque/OpaqueException");
  env->ThrowNew(exception, message);
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_Init(
  // FIXME: Remove last jbyteArray parameter - it was for testing purposes
  // JNIEnv *env, jobject obj, jbyteArray shared_key, jstring intel_cert, jstring user_cert, jbyteArray key_share, jbyteArray test_key) {
  JNIEnv *env, jobject obj, jbyteArray shared_key, jstring intel_cert, jstring user_cert, jbyteArray key_share) {
  (void)env;
  (void)obj;

  jboolean if_copy = false;
  jbyte *shared_key_bytes = env->GetByteArrayElements(shared_key, &if_copy);

  jbyte *key_share_bytes = env->GetByteArrayElements(key_share, &if_copy);

  const char *intel_cert_str = env->GetStringUTFChars(intel_cert, nullptr);
  //size_t intel_cert_len = static_cast<size_t>(env->GetStringUTFLength(intel_cert));
  //
  const char* user_cert_str = env->GetStringUTFChars(user_cert, nullptr);
  // size_t user_cert_len = static_cast<size_t>(env->GetStringUTFLength(user_cert));

  try {
    // const char *private_key_path = std::getenv("PRIVATE_KEY_PATH");
    // if (!private_key_path) {
    //   throw std::runtime_error(
    //     "Set $PRIVATE_KEY_PATH to the file generated by "
    //     "openssl ecparam -genkey, probably called ${OPAQUE_HOME}/private_key.pem.");
    // }
    // service_provider.load_private_key(private_key_path);
    service_provider.set_shared_key(reinterpret_cast<uint8_t *>(shared_key_bytes));

    // THIS BLOCK FOR TESTING PURPOSES
    // jbyte *test_key_bytes = env->GetByteArrayElements(test_key, &if_copy);
    // service_provider.set_test_key(reinterpret_cast<uint8_t *>(test_key_bytes));

    // set user certificate
    service_provider.set_user_cert(user_cert_str);

    // set key share
    service_provider.set_key_share(reinterpret_cast<uint8_t *>(key_share_bytes));
    //service_provider.connect_to_ias(std::string(intel_cert_str, intel_cert_len));
  } catch (const std::runtime_error &e) {
    jni_throw(env, e.what());
  }

  env->ReleaseByteArrayElements(shared_key, shared_key_bytes, 0);
  env->ReleaseStringUTFChars(intel_cert, intel_cert_str);
  env->ReleaseStringUTFChars(user_cert, user_cert_str);
}


// JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg0(
//   JNIEnv *env, jobject obj, jbyteArray msg0_input) {
//   (void)obj;

//   jboolean if_copy = false;
//   jbyte *msg0_bytes = env->GetByteArrayElements(msg0_input, &if_copy);
//   uint32_t *extended_epid_group_id = reinterpret_cast<uint32_t *>(msg0_bytes);

//   try {
//     service_provider.process_msg0(*extended_epid_group_id);
//   } catch (const std::runtime_error &e) {
//     jni_throw(env, e.what());
//   }  

//   env->ReleaseByteArrayElements(msg0_input, msg0_bytes, 0);
// }

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg1(
  JNIEnv *env, jobject obj, jbyteArray msg1_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *msg1_bytes = env->GetByteArrayElements(msg1_input, &if_copy);
  oe_msg1_t *msg1 = reinterpret_cast<oe_msg1_t *>(msg1_bytes);

  uint32_t msg2_size = 0;
  std::unique_ptr<oe_msg2_t> msg2;
  try {
    msg2 = service_provider.process_msg1(msg1, &msg2_size);
  } catch (const std::runtime_error &e) {
    jni_throw(env, e.what());
  }

  jbyteArray array_ret = env->NewByteArray(msg2_size);
  env->SetByteArrayRegion(array_ret, 0, msg2_size, reinterpret_cast<jbyte *>(msg2.get()));

  env->ReleaseByteArrayElements(msg1_input, msg1_bytes, 0);

  return array_ret;
}

// JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg3(
//   JNIEnv *env, jobject obj, jbyteArray msg3_input) {
//   (void)obj;

//   jboolean if_copy = false;
//   jbyte *msg3_bytes = env->GetByteArrayElements(msg3_input, &if_copy);
//   sgx_ra_msg3_t *msg3 = reinterpret_cast<sgx_ra_msg3_t *>(msg3_bytes);
//   uint32_t msg3_size = static_cast<uint32_t>(env->GetArrayLength(msg3_input));

//   uint32_t msg4_size = 0;
//   std::unique_ptr<ra_msg4_t> msg4;
//   try {
//     msg4 = service_provider.process_msg3(msg3, msg3_size, &msg4_size);
//   } catch (const std::runtime_error &e) {
//     jni_throw(env, e.what());
//   }

//   jbyteArray ret = env->NewByteArray(msg4_size);
//   env->SetByteArrayRegion(ret, 0, msg4_size, reinterpret_cast<jbyte *>(msg4.get()));

//   env->ReleaseByteArrayElements(msg3_input, msg3_bytes, 0);

//   return ret;
// }
