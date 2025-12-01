import { Inject, Injectable } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import * as bcrypt from 'bcryptjs';
import { Db } from 'mongodb';

export enum UserRole {
  SUPER_ADMIN = 1,
  ACCOUNT = 2,
}

@Injectable()
export class AuthService {
  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    private readonly jwtService: JwtService
  ) {}

  private validatePasswordStrength(password: string) {
    const rule =
      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/;

    if (!rule.test(password)) {
      throw new Error(
        'Password must be at least 8 characters long and include uppercase, lowercase, number, and special character.'
      );
    }
  }

  async register(name: string, email: string, password: string, mobile: string, role: UserRole) {
    try {
      if (!name || !email || !password || !mobile || !role) {
        return {
          data: null,
          message: { msg: 'Missing required fields.', code: 0 },
        };
      }

      this.validatePasswordStrength(password);

      const users = this.db.collection('users');
      const existing = await users.findOne({ email });

      if (existing) {
        return {
          data: null,
          message: { msg: 'Email already registered.', code: 0 },
        };
      }

      const hashedPassword = await bcrypt.hash(password, 10);

      const user = {
        name,
        email,
        mobile,
        role,
        password: hashedPassword,
        previousPasswords: [],
        token: null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };

      await users.insertOne(user);

      return {
        data: { email },
        message: { msg: 'User registered successfully.', code: 1 },
      };
    } catch (error: any) {
      return {
        data: null,
        message: { msg: error.message || 'Registration failed', code: 0 },
      };
    }
  }

  async login(email: any, password: any) {
    try {
      const users = this.db.collection('users');
      const user = await users.findOne({ email });

      if (!user) throw new Error('Invalid email or password.');

      const passwordValid = await bcrypt.compare(password, user.password);
      if (!passwordValid) throw new Error('Invalid email or password.');

      const payload = { email: user.email, role: user.role };
      const token = this.jwtService.sign(payload);

      await users.updateOne(
        { email },
        {
          $set: {
            token,
            updatedAt: new Date(),
          },
        }
      );

      return {
        data: {access_token:token},
        message: { msg: "Login Successful", code: 1 },
      };
    } catch (error: any) {
      return {
        data: null,
        message: { msg: error.message || 'Login failed', code: 0 },
      };
    }
  }

  async updatePassword(email: string, newPassword: string) {
    try {
      const users = this.db.collection('users');
      const user = await users.findOne({ email });

      if (!user) {
        return {
          data: null,
          message: { msg: 'User not found.', code: 0 },
        };
      }

      this.validatePasswordStrength(newPassword);

      const allPasswords = [user.password, ...(user.previousPasswords || [])];
      const match = await Promise.all(
        allPasswords.map((oldPass) => bcrypt.compare(newPassword, oldPass))
      );

      if (match.includes(true)) {
        return {
          data: null,
          message: { msg: 'You cannot reuse your last 3 passwords.', code: 0 },
        };
      }

      const hashed = await bcrypt.hash(newPassword, 10);
      const updatedPrevious = [user.password, ...(user.previousPasswords || [])];
      if (updatedPrevious.length > 3) updatedPrevious.pop();

      await users.updateOne(
        { email },
        {
          $set: {
            password: hashed,
            previousPasswords: updatedPrevious,
            updatedAt: new Date(),
          },
        }
      );

      return {
        data: { email },
        message: { msg: 'Password updated successfully.', code: 1 },
      };
    } catch (error: any) {
      return {
        data: null,
        message: { msg: error.message || 'Password update failed', code: 0 },
      };
    }
  }

  async validateUser(payload: any) {
    return {
      data: { email: payload.email, role: payload.role },
      message: { msg: 'User validated successfully.', code: 1 },
    };
  }
}
