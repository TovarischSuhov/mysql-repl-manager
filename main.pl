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
# REQUIREMENTS: Config::Tiny, AnyEvent, AnyEvent::Loop, AnyEvent::Handle::UDP, DBI, DBD::mysql
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
use DBI;
use DBD::mysql;

sub onrecieve;
sub dbcheck;
sub check_slave_status;
sub get_address;
sub get_ipv4;
sub get_ipv6;
sub switch_to;

### Reading config

my $config = Config::Tiny->new;
$config = Config::Tiny->read( 'mysql_repl.conf' );

my %selfstatus;
my %sql;

$selfstatus{id} = $config->{settings}->{id};
$selfstatus{hostname} = $config->{settings}->{hostname};
$selfstatus{protocol} = $config->{settings}->{protocol};
$selfstatus{maxdelay} = $config->{settings}->{maxdelay};
$selfstatus{port} = $config->{settings}->{port};

$selfstatus{port} = 4000 if not defined $selfstatus{port};
$selfstatus{protocol} = "IPv4" if not defined $selfstatus{protocol};
$selfstatus{maxdelay} = 20 if not defined $selfstatus{maxdelay};

$sql{user} = $config->{sql}->{user};
$sql{passwd} = $config->{sql}->{passwd};
$sql{root} = $config->{sql}->{root};
$sql{rootpasswd} = $config->{sql}->{rootpasswd};

$selfstatus{address} = get_address($selfstatus{hostname});

my %hosts;
for my $x(keys %{$config}){
	if($x =~ /host\d+/){
	$hosts{$config->{$x}->{id}}->{address} = get_address($config->{$x}->{address});
	$hosts{$config->{$x}->{id}}->{status} = "PING_OK";
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

my $dbh = DBI->connect("DBI:mysql:mysql",$sql{root},$sql{rootpasswd});
my $sth = $dbh->prepare("SHOW SLAVE STATUS");
$sth->execute();
my $ref = $sth->fetchrow_hashref();

if((not defined $ref) or ($ref->{Master_Host} eq $selfstatus{hostname})){
	$selfstatus{role} = "master";
} 
else{
	$selfstatus{role} = "slave";
}

$dbh->disconnect;

for my $x (keys %hosts){
	$udp->push_send(qq($selfstatus{id}:ASK_INFO;),$hosts{$x}->{address});
	$udp->push_send(qq($selfstatus{id}:INFO;$selfstatus{role}),$hosts{$x}->{address})
}

sub check_db{
	my $dbh = DBI->connect("DBI:mysql:mysql",
							$sql{root}, $sql{rootpasswd},{RaiseError => 0});
	return 0 if not defined $dbh;
	eval{$dbh->do("SELECT * FROM user")};
	return 0 if $@;
	$dbh->disconnect;
	return 1;
}

sub check_slave_status{
	for my $x(keys %hosts){
		if($hosts{$x}->{status} eq "DB_DOWN"){
			warn "Host #$x is droped"
		}
		elsif(AnyEvent->now - $hosts{$x}->{time} > $selfstatus{maxdelay}){
		$hosts{$x}->{status} = "DB_DOWN";
		warn "Host #$x is droped now";
		}
		else{warn "Host #$x is OK"};
	}
}

sub dbcheck{
	my $time = AnyEvent->now;
	if(check_db){ # check db status
		for my $x(keys %hosts){
#			warn qq(Send PING_OK to $x, $hosts{$x}->{address}->[0], $hosts{$x}->{address}->[1]);
			$udp->push_send(qq($selfstatus{id}:PING_OK;$time),$hosts{$x}->{address});
		}
	}
	else{
		for my $x(keys %hosts){
#			warn qq(Send DB_DOWN to $x, $hosts{$x}->{address}->[0], $hosts{$x}->{address}->[1]);
			$udp->push_send(qq($selfstatus{id}:DB_DOWN;$time),$hosts{$x}->{address});
		}	
	}
}

sub onrecieve{
	my ($data, $handle, $addr) = @_;
	$data =~ /(\d+):([_A-Z]+);(.*)?/;
	my($id ,$status, $args) = ($1, $2, $3);
	if($status eq "PING_OK"){ # status ok
		$hosts{$id}->{"status"} = $status;
		$hosts{$id}->{time} = $args;
#		warn qq($id $hosts{$id}->{status} $hosts{$id}->{time}); # debuging info
	}
	elsif($status eq "DB_DOWN"){ # db is down
		$hosts{$id}->{status} = $status;
#		warn qq($id $hosts{$id}->{status} $hosts{$id}->{time}); # debuging info
	}
	elsif($status eq "SWITCH_TO"){ # command switch
		my($host, $file, $position) = split /;/,$args;
		switch_to($host, $file,$position);
	}
	elsif($status eq "LOAD_FROM"){ # command to load

	}
	elsif($status eq "SWITCH_OVER"){ # command to master to switch_over

	}
	elsif($status eq "ASK_INFO"){
		$udp->push_send(qq($selfstatus{id}:INFO;$selfstatus{role}),$hosts{$id}->{address});
	}
	elsif($status eq "INFO"){
		$hosts{$id}->{role} = $args;
	}
	elsif($status eq "VOTE"){

	}
	else{
		
	}	
}

sub get_address{
	my $address;
	my $hostname = shift;
	if($selfstatus{protocol} eq "IPv6"){
			my $tmp = get_ipv6($hostname);
			if (!defined $tmp){
				$selfstatus{protocol} = "IPv4";
				$address = get_address($hostname);
			}
			return [$tmp,$selfstatus{port}]
	}
	elsif($selfstatus{protocol} eq "IPv4"){
		   return [get_ipv4($hostname), $selfstatus{port}];
	}
	else{
		warn qq(Unknown protocol $selfstatus{protocol})
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

sub switch_to{
	my $hostname = shift;
	my $filename = shift;
	my $position = shift;
	my $query = qq(change master to master_host="$hostname", master_user="$sql{user}", master_password="$sql{passwd}", master_log_file="$filename", master_log_pos=$position);
	my $dbh = DBI->connect("DBI:mysql:mysql", $sql{root}, $sql{rootpasswd});
	eval {$dbh->do($query)};
	$@ || warn qq(Couldn't done $query);
	$dbh->disconnect;
}

AnyEvent::Loop::run; # main loop


__END__

=head3 STATUSES

	ID:PING_OK;[time] - regulary comes, sends creation times

	ID:DB_DOWN;[time] - shows when DB on sender is down 

	ID:SWITCH_TO;[host;file;pos] - commands to switch master to host

	ID:LOAD_FROM;[host] - commands to load DB dump from host

	ID:SWITCH_OVER;[host] - commands master to switch master to host

	ID:ASK_INFO; - asks about roles

	ID:INFO;[role] - tells role (master, slave)

	ID:VOTE;[id] - votes for host with id
