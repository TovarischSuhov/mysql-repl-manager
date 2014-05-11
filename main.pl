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
# REQUIREMENTS: ---
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

### Reading config

my $config = Config::Tiny->new;
$config = Config::Tiny->read( 'mysql_repl.conf' );

my %selfstatus;

$selfstatus{"id"} = $config->{settings}->{id};
$selfstatus{"hostname"} = $config->{settings}->{hostaname};
$selfstatus{"port"} = $config->{settings}->{address};
$selfstatus{"protocol"} = $config->{settings}->{protocol};

$selfstatus{"port"} = 4000 if not defined $selfstatus{"port"};
$selfstatus{"protocol"} = "IPv4" if not defined $selfstatus{"protocol"};

my %hosts;
$hosts{"1"}->{"address"} = ["127.0.0.1",4000];
$hosts{"1"}->{"status"} = "";
$hosts{"1"}->{"time"} = "";

my $signal = AnyEvent->signal (signal => "TERM", cb => sub {exit 0});

my $udp = AnyEvent::Handle::UDP->new(
	bind => ["0.0.0.0", 4000],
	on_recv => \&onrecieve,
);

my $dbcheck = AnyEvent->timer(interval => 5, cb => \&dbcheck);
my $check_slaves = AnyEvent->timer(interval => 5, cb => \&check_slave_status);

###Here would be initialization part
my $address = get_address;
$udp->push_send(qq($selfstatus{"id"}:ASK_HOSTS;$address->[0];$address->[1];slave),);



sub check_db{
	return 1;
}

sub check_slave_status{
	for my $x(keys %hosts){
		warn $x;
	}
}

sub dbcheck{
	my $time = AnyEvent->now;
	if(check_db){ # check db status
		for my $x(keys %hosts){
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
		warn qq($hosts{$id}->{"status"} $hosts{$id}->{"time"}); # debuging info
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
	my $hostname = $selfstatus{"hostname"};
	my @ans = `host $hostname`;
	if($selfstatus{"protocol"} eq "IPv6"){
			
	}
	elsif($selfstatus{"protocol"} eq "IPv4"){
		
	}
	else{
		warn qq(Unknown protocol $selfstatus{"protocol"})
	}
	return $address;
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
	
