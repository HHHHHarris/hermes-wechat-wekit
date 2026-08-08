#!/bin/sh
# Reach the phone's WeKit API from a host on a different subnet, by DNAT on the
# router the phone is connected to.
#
# Use this when the agent host and the phone are on different networks (for
# example two routers hanging off one modem). If they share a subnet you do not
# need this at all — just point WEKIT_BASE_URL straight at the phone.
#
#   agent host ---> ROUTER_ADDR:PORT ---(DNAT)---> phone_ip:PORT
#
# Why not USB/adb: on the reference deployment `adb forward` broke roughly every
# 21 seconds because the host's adb server kept crashing and respawning, taking
# the forward with it. Moving to this path took long-poll failures from ~170/hour
# to zero.
#
# The phone's address is resolved from the DHCP lease file each run, so a lease
# change repairs itself. Rules are tagged with a comment and only tagged rules
# are ever removed, so this will not disturb other firewall rules (proxy/VPN
# rules included).
#
# Tested on OpenWrt-family firmware (busybox sh, iptables). Run from a boot hook
# and periodically, e.g.:
#   /etc/rc.local:  (sleep 30; sh /etc/wekit-dnat.sh) &
#   cron:           */2 * * * * sh /etc/wekit-dnat.sh >/dev/null 2>&1
#
# ============================ configuration ============================
# Required. Address on THIS router that the agent host will connect to (usually
# its WAN address on the shared upstream segment). No default on purpose: a
# plausible-looking wrong address would DNAT to somebody else's host.
ROUTER_ADDR="${WEKIT_ROUTER_WAN:-}"

# Required. The phone's hostname exactly as it appears in the DHCP lease file
# (check with:  cat /var/dhcp.leases  ).
PHONE_HOSTNAME="${WEKIT_PHONE_HOSTNAME:-}"

# WeKit's port on the phone.
PORT="${WEKIT_PHONE_PORT:-3001}"

# STRONGLY RECOMMENDED: restrict who may reach the phone through this rule to
# the agent host's address. Left empty, ANY host that can route to ROUTER_ADDR
# gets full read/send control of the WeChat account, protected only by a bearer
# token that travels in cleartext.
ALLOW_SRC="${WEKIT_ALLOW_SRC:-}"

LEASES="${WEKIT_DHCP_LEASES:-/var/dhcp.leases}"
TAG=wekit-dnat
# =======================================================================

if [ -z "$ROUTER_ADDR" ] || [ -z "$PHONE_HOSTNAME" ]; then
    echo "wekit-dnat: set WEKIT_ROUTER_WAN and WEKIT_PHONE_HOSTNAME (or edit the" >&2
    echo "            configuration block at the top of this script)." >&2
    exit 2
fi

PHONE=$(awk -v n="$PHONE_HOSTNAME" '$4==n{print $3}' "$LEASES" 2>/dev/null | head -1)
if [ -z "$PHONE" ]; then
    logger -t "$TAG" "no DHCP lease for '$PHONE_HOSTNAME' in $LEASES; leaving rules unchanged"
    exit 0
fi

# Already pointing at the right address: do nothing. Rebuilding rules needlessly
# would sever the agent's in-flight long poll.
if iptables -t nat -S PREROUTING 2>/dev/null |
        grep -q -- "--comment $TAG .*--to-destination $PHONE:$PORT"; then
    exit 0
fi

# Remove only our own previous rules (highest index first so indices stay valid).
for n in $(iptables -t nat -L PREROUTING --line-numbers -n 2>/dev/null |
           grep "$TAG" | awk '{print $1}' | sort -rn); do
    iptables -t nat -D PREROUTING "$n" 2>/dev/null
done
for n in $(iptables -t nat -L POSTROUTING --line-numbers -n 2>/dev/null |
           grep "$TAG" | awk '{print $1}' | sort -rn); do
    iptables -t nat -D POSTROUTING "$n" 2>/dev/null
done
for n in $(iptables -L FORWARD --line-numbers -n 2>/dev/null |
           grep "$TAG" | awk '{print $1}' | sort -rn); do
    iptables -D FORWARD "$n" 2>/dev/null
done

if [ -n "$ALLOW_SRC" ]; then
    SRC="-s $ALLOW_SRC"
else
    SRC=""
    logger -t "$TAG" "WARNING: WEKIT_ALLOW_SRC unset - the phone's WeKit API is reachable by any host that can route to $ROUTER_ADDR"
fi

# shellcheck disable=SC2086  # $SRC is an intentional optional argument pair
iptables -t nat -I PREROUTING 1 $SRC -d "$ROUTER_ADDR" -p tcp --dport "$PORT" \
    -m comment --comment "$TAG" -j DNAT --to-destination "$PHONE:$PORT"
# shellcheck disable=SC2086
iptables -I FORWARD 1 $SRC -d "$PHONE" -p tcp --dport "$PORT" \
    -m comment --comment "$TAG" -j ACCEPT
# Make replies come back through the router rather than going directly to a
# source the phone has no route to.
iptables -t nat -I POSTROUTING 1 -d "$PHONE" -p tcp --dport "$PORT" \
    -m comment --comment "$TAG" -j MASQUERADE

logger -t "$TAG" "DNAT $ROUTER_ADDR:$PORT -> $PHONE:$PORT installed${ALLOW_SRC:+ (source restricted to $ALLOW_SRC)}"
