package io.findify.s3mock.alpakka

import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.findify.s3mock.S3MockTest

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by shutty on 5/19/17.
  */
class MultipartUploadTest extends S3MockTest {

  override def behaviour(fixture: => Fixture): Unit = {
    val s3 = fixture.client
    implicit val sys = fixture.system

    it should "upload multipart files" in {
      s3.createBucket("alpakka1")

      val result = Await.result(
        Source
          .single(ByteString("testcontent1"))
          .runWith(
            S3.multipartUpload("alpakka1", "test1")
              .withAttributes(S3Attributes.settings(fixture.alpakka))
          ),
        5.seconds
      )

      result.bucket shouldBe "alpakka1"
      result.key shouldBe "test1"

      getContent(s3.getObject("alpakka1", "test1")) shouldBe "testcontent1"
    }

  }
}
