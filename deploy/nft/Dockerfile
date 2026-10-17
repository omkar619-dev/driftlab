# The relay driver's cut fault (drivers/relay) runs nft from this image, in a
# throwaway container that joins a broker's network namespace
# (--network container:kafka-N) with the right to change its firewall
# (--cap-add NET_ADMIN). Build it once, on the machine that runs the brokers:
#
#	docker build -t driftlab-nft deploy/nft
FROM alpine:3.22
RUN apk add --no-cache nftables
