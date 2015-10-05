package examples.actors

import java.util.Date

import scala.actors._
import scala.actors.Actor._

/** A simple demonstrator program implementing an online auction service
 *  The example uses the actor abstraction defined in the API of
 *  package scala.actors.
 */

trait AuctionMessage
case class Offer(bid: Int, client: Actor) extends AuctionMessage // make a bid
case class Inquire(client: Actor) extends AuctionMessage         // inquire status

trait AuctionReply
case class Status(asked: Int, expiration: Date)           // asked sum, expiration date
  extends AuctionReply
case object BestOffer extends AuctionReply                // yours is the best offer
case class BeatenOffer(maxBid: Int) extends AuctionReply  // offer beaten by maxBid
case class AuctionConcluded(seller: Actor, client: Actor) // auction concluded
  extends AuctionReply
case object AuctionFailed extends AuctionReply            // failed with no bids
case object AuctionOver extends AuctionReply              // bidding is closed

class AuctionActor(seller: Actor, minBid: Int, closing: Date) extends Actor {
  val timeToShutdown = 3000 // msec
  val bidIncrement = 10

  def act() {
    var maxBid = minBid - bidIncrement
    var maxBidder: Actor = null

    loop {
      reactWithin (closing.getTime() - new Date().getTime()) {

        case Offer(bid, client) =>
          if (bid >= maxBid + bidIncrement) {
            if (maxBid >= minBid)
              maxBidder ! BeatenOffer(bid)
            maxBid = bid
            maxBidder = client
            client ! BestOffer
          }
          else
            client ! BeatenOffer(maxBid)

        case Inquire(client) =>
          client ! Status(maxBid, closing)

        case TIMEOUT =>
          if (maxBid >= minBid) {
            val reply = AuctionConcluded(seller, maxBidder)
            maxBidder ! reply
            seller ! reply
          } else {
            seller ! AuctionFailed
          }
          reactWithin(timeToShutdown) {
            case Offer(_, client) => client ! AuctionOver
            case TIMEOUT => exit()
          }

      }
    }
  }
}

object auction {

  val random = new scala.util.Random

  val minBid = 100
  val closing = new Date(new Date().getTime() + 4000)

  val seller = Actor.actor { }
  val auction = new AuctionActor(seller, minBid, closing)

  def client(i: Int, increment: Int, top: Int) = new Actor {
    val name = "Client " + i
    def log(msg: String) = println(name + ": " + msg)
    var max: Int = _
    var current: Int = 0
    def act() {
      log("started")
      auction ! Inquire(this)
      receive {
        case Status(maxBid, _) =>
          log("status(" + maxBid + ")")
          max = maxBid
      }
      loop {
        if (max >= top) {
          log("too high for me")
        }
        else if (current < max) {
          current = max + increment
          Thread.sleep(1 + random.nextInt(1000))
          auction ! Offer(current, this)
        }

        reactWithin(3000) {
          case BestOffer =>
            log("bestOffer(" + current + ")")

          case BeatenOffer(maxBid) =>
            log("beatenOffer(" + maxBid + ")")
            max = maxBid

          case AuctionConcluded(seller, maxBidder) =>
            log("auctionConcluded"); exit()

          case AuctionOver =>
            log("auctionOver"); exit()

          case TIMEOUT =>
            exit()
        }
      }
    }
  }

  def main(args: Array[String]) {
    seller.start()
    auction.start()
    client(1, 20, 200).start()
    client(2, 10, 300).start()
  }

}
