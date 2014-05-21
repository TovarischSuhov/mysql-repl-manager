#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: main.pl
#
#        USAGE: ./main.pl  
#
#  DESCRIPTION: 
#
#      OPTIONS: ---
# REQUIREMENTS: Config::Tiny, AnyEvent, AnyEvent::Loop, AnyEvent::Handle::UDP
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Ilya Suhov (), tovarischsuhov0@yandex.ru
# ORGANIZATION: 
#      VERSION: 1.0
#      CREATED: 10.05.2014 17:58:41
#     REVISION: ---
#===============================================================================

use strict;
use warnings;
use utf8;
use Config::Tiny;
use AnyEvent;
use AnyEvent::Loop;
use AnyEvent::Handle::UDP;

sub onrecieve;
sub dbcheck;
sub check_slave_status;
sub get_address;
sub get_ipv4;
sub get_ipv6;

### Reading config

my $config = Config::Tiny->new;
$config = Config::Tiny->read( 'mysql_repl.conf' );

my %selfstatus;

$selfstatus{"id"} = $config->{settings}->{id};
$selfstatus{"hostname"} = $config->{settings}->{hostname};
$selfstatus{"protocol"} = $config->{settings}->{protocol};
$selfstatus{maxdelay} = $config->{settings}->{maxdelay};

$selfstatus{"port"} = 4000 if not defined $selfstatus{"port"};
$selfstatus{"protocol"} = "IPv4" if not defined $selfstatus{"protocol"};
$selfstatus{maxdelay} = 20 if not defined $selfstatus{maxdelay};


$selfstatus{"address"} = get_address($selfstatus{"hostname"});

my %hosts;
for my $x(keys %{$config}){
	if($x =~ /host\d+/){
	$hosts{$config->{$x}->{id}}->{address} = get_address($config->{$x}->{address});
	}
}

my $signal = AnyEvent->signal (signal => "TERM", cb => sub {exit 0});

my $udp = AnyEvent::Handle::UDP->new(
	bind => ["0.0.0.0", $selfstatus{"port"}],
	on_recv => \&onrecieve,
);

my $dbcheck = AnyEvent->timer(interval => 5, cb => \&dbcheck);
my $check_slaves = AnyEvent->timer(interval => 5, cb => \&check_slave_status);

###Here would be initialization part

sub check_db{
	return 1;
}

sub check_slave_status{
	for my $x(keys %hosts){
		if(AnyEvent->now - $hosts{$x}->{time} > $selfstatus{maxdelay}){
		warn "Host #$x is droped now";
		}
		else{warn "Host #$x is OK"};
	}
}

sub dbcheck{
	my $time = AnyEvent->now;
	if(check_db){ # check db status
		for my $x(keys %hosts){
			warn qq(Send PING_OK to $x, $hosts{$x}->{"address"}->[0], $hosts{$x}->{"address"}->[1]);
			$udp->push_send(qq($selfstatus{"id"}:PING_OK;$time),$hosts{$x}->{"address"});
		}
	}
	else{
		for my $x(keys %hosts){
			$udp->push_send(qq($selfstatus{"id"}:DB_DOWN;$time),$hosts{$x}->{"address"});
		}	
	}
}

sub onrecieve{
	my ($data, $handle, $addr) = @_;
	$data =~ /(\d+):([_A-Z]+);(.*)/;
	my($id ,$status, $args) = ($1, $2, $3);
	if($status eq "PING_OK"){ # status ok
		$hosts{$id}->{"status"} = $status;
		$hosts{$id}->{"time"} = $args;
		warn qq($id $hosts{$id}->{"status"} $hosts{$id}->{"time"}); # debuging info
	}
	elsif($status eq "DB_DOWN"){ # db is down
	
	}
	elsif($status eq "SWITCH_TO"){ # command switch

	}
	elsif($status eq "LOAD_FROM"){ # command to load

	}
	elsif($status eq "SWITCH_OVER"){ # command to master to switch_over

	}
	elsif($status eq "HOST_INFO"){ # send host info

	}
	elsif($status eq "ASK_HOSTS"){ # asks for hosts info

	}
	else{
		
	}	
}

sub get_address{
	my $address;
	my $hostname = shift;
	if($selfstatus{"protocol"} eq "IPv6"){
			my $tmp = get_ipv6($hostname);
			if (!defined $tmp){
				$selfstatus{"protocol"} = "IPv4";
				$address = get_address($hostname);
			}
			return [$tmp,$selfstatus{"port"}]
	}
	elsif($selfstatus{"protocol"} eq "IPv4"){
		   return [get_ipv4($hostname), $selfstatus{"port"}];
	}
	else{
		warn qq(Unknown protocol $selfstatus{"protocol"})
	}
	return $address;
}

sub get_ipv4{
	my $hostname = shift;
 	if($hostname =~ /^((?:\d{1,3}\.){3}\d{1,3})$/){return $hostname}
	my $hosts = `host $hostname`;
	$hosts =~ /((?:\d{1,3}\.){3}\d{1,3})/m;
	return $1;
}

sub get_ipv6{
	my $hostname = shift;
	if($hostname =~ /^([0-9a-f:]{4,})$/){return $hostname}
	my $hosts = `host $hostname`;
	$hosts =~ /([0-9a-f:]{4,})/m;
	return $1;
}

AnyEvent::Loop::run; # main loop


__END__

=head3 STATUSES

	ID:PING_OK;[time] - regulary comes, sends creation times

	ID:DB_DOWN;[time] - shows when DB on sender is down 

	ID:SWITCH_TO;[host] - commands to switch master to host

	ID:LOAD_FROM;[host] - commands to load DB dump from host

	ID:SWITCH_OVER;[host] - commands master to switch master to host

	ID:HOST_INFO;[address;port;role] - sends hosts info

	ID:ASK_HOSTS;[address;port;role] - asks for hosts info, and send my info
	
