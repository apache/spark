import 'package:flutter/material.dart';
import 'screens/buyer_login.dart';
import 'screens/seller_login.dart';

void main() {
  runApp(IndiaMartCloneApp());
}

class IndiaMartCloneApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'IndiaMart Clone',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: RoleSelectionScreen(),
    );
  }
}

class RoleSelectionScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('IndiaMart Clone')),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            ElevatedButton(
              child: Text('Login as Buyer'),
              onPressed: () =>
                  Navigator.push(context, MaterialPageRoute(builder: (_) => BuyerLoginScreen())),
            ),
            ElevatedButton(
              child: Text('Login as Seller'),
              onPressed: () =>
                  Navigator.push(context, MaterialPageRoute(builder: (_) => SellerLoginScreen())),
            ),
          ],
        ),
      ),
    );
  }
}