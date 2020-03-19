package io.findify.s3mock.alpakka

import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Sink
import io.findify.s3mock.S3MockTest

import scala.concurrent.Await
import scala.concurrent.duration._

class ListBucketTest extends S3MockTest {
  override def behaviour(fixture: => Fixture): Unit = {
    val s3 = fixture.client
    implicit val sys = fixture.system

    it should "list objects via alpakka" in {
      s3.createBucket("alpakkalist")
      s3.putObject("alpakkalist", "test1", "foobar")
      s3.putObject("alpakkalist", "test2", "foobar")
      s3.putObject("alpakkalist", "test3", "foobar")
      val result = Await.result(
        S3.listBucket("alpakkalist", None)
          .withAttributes(S3Attributes.settings(fixture.alpakka))
          .runWith(Sink.seq),
        5.second
      )

      result.size shouldBe 3
      result.map(_.key) shouldBe Seq("test1", "test2", "test3")
    }

    it should "list objects with prefix" in {
      s3.createBucket("alpakkalist2")
      s3.putObject("alpakkalist2", "test1", "foobar")
      s3.putObject("alpakkalist2", "test2", "foobar")
      s3.putObject("alpakkalist2", "xtest3", "foobar")
      val result = Await.result(
        S3.listBucket("alpakkalist2", Some("test"))
          .withAttributes(S3Attributes.settings(fixture.alpakka))
          .runWith(Sink.seq),
        5.second
      )

      result.size shouldBe 2
      result.map(_.key) shouldBe Seq("test1", "test2")
    }
  }
}
